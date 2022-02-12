package gnode

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"impact-eintr/esq/pkg/logs"
	"impact-eintr/esq/pkg/utils"
	"io/ioutil"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/impact-eintr/bolt"
)

const (
	DEFAULT_KEY           = "default"
	DEAD_QUEUE_FLAG       = "dead"
	ROUTE_KEY_MATCH_FULL  = 1
	ROUTE_KEY_MATCH_FUZZY = 2
)

var (
	ErrNotMultiple    = errors.New("this topic is not a multiple topic")
	ErrHaveRegistered = errors.New("this client have registered")
)

// topic 消息主题,即消息类型,每一条消息都有所属topic,topic会维护多个queue
// topic 的所有数据会存放在 他自身拥有的 Dispatcher 的DB对象的一个桶中(topic.name 为该桶的 key)
type Topic struct {
	name       string
	mode       int
	msgTTR     int // 消息有效时间
	msgRetry   int
	isAutoAck  bool // 自动回复表示每条消息不会等待回复
	isMultiple bool // 是否是多播频道

	pushNum int64
	popNum  int64
	deadNum int64

	// 状态管理
	startTime  time.Time
	closed     bool
	ctx        *Context // 内含 gnode config topic_dospatcher logger
	dispatcher *Dispatcher
	wg         utils.WaitGroupWrapper
	exitChan   chan struct{}

	// 内部队列 与 锁
	queues     map[string]*queue
	deadQueues map[string]*queue
	waitAckMux sync.Mutex
	queueMux   sync.Mutex
	sync.Mutex
}

type topicConfigure struct {
	isAutoAck int // 是否自动确认消息，1是，0否，默认为0
	msgTTR    int // 消息执行超时时间，在msgTTR内没有确认消息，则消息重新入队，再次被消费,默认为30
	msgRetry  int // 消息重试次数，超过msgRetry次数后，消息会被写入到死信队列，默认为5
	mode      int // 路由key匹配模式，1全匹配，2模糊匹配，默认为1
}

type TopicMeta struct {
	Mode        int         `json:"mode"`
	PopNum      int64       `json:"pop_num"`
	PushNum     int64       `json:"push_num"`
	DeadNum     int64       `json:"dead_num"`
	IsAutoAck   bool        `json:"is_auto_ack"`
	Queues      []QueueMeta `json:"queues"`
	DeataQueues []QueueMeta `json:"dead_queues"`
}

type QueueMeta struct {
	Num         int64  `sjon:"queue_num"`    // 队列长度
	Name        string `sjon:"queue_name"`   // 队列名字
	BindKey     string `json:"bind_key"`     // 队列绑定的唯一值
	WriteOffset int64  `json:"write_offset"` // 写指针
	ReadOffset  int64  `json:"read_offset"`  // 读指针
	ScanOffset  int64  `json:"scan_offset"`  // 扫描指针
}

func NewTopic(name string, ctx *Context) *Topic {
	t := &Topic{
		ctx:        ctx,
		name:       name,
		msgTTR:     ctx.Conf.MsgTTR,
		msgRetry:   ctx.Conf.MsgMaxRetry,
		mode:       ROUTE_KEY_MATCH_FUZZY,
		isAutoAck:  true,
		isMultiple: false,
		exitChan:   make(chan struct{}),
		dispatcher: ctx.Dispatcher,
		startTime:  time.Now(),
		queues:     make(map[string]*queue),
		deadQueues: make(map[string]*queue),
	}

	t.init()
	return t
}

// 初始化
func (t *Topic) init() {
	err := t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(t.name)); err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	t.LogInfo(fmt.Sprintf("loading topic.%s metadata.", t.name))

	// 初始化元数据
	fp, err := os.OpenFile(fmt.Sprintf("%s/%s.meta", t.ctx.Conf.DataSavePath, t.name), os.O_RDONLY, 0600)
	if err != nil {
		if !os.IsNotExist(err) {
			t.LogError(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		}
		return
	}
	defer fp.Close()

	data, err := ioutil.ReadAll(fp)
	if err != nil {
		t.LogError(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		return
	}
	meta := &TopicMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		t.LogError(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		return
	}

	// restore topic meta data
	t.mode = meta.Mode
	t.popNum = meta.PopNum
	t.pushNum = meta.PushNum
	t.deadNum = meta.DeadNum
	t.isAutoAck = meta.IsAutoAck

	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	for _, q := range meta.Queues {
		// skip empty queue
		if q.Num == 0 {
			continue
		}
		queue := NewQueue(q.Name, q.BindKey, t.ctx, t)
		queue.woffset = q.WriteOffset
		queue.roffset = q.ReadOffset
		queue.soffset = q.ScanOffset
		queue.num = q.Num
		queue.name = q.Name
		t.queues[q.BindKey] = queue

		t.LogInfo(fmt.Sprintf("restore queue %s", queue.name))
	}

	// restore dead queue meta data
	for _, q := range meta.DeataQueues {
		// skip empty queue
		if q.Num == 0 {
			continue
		}
		queue := NewQueue(q.Name, q.BindKey, t.ctx, t)
		queue.woffset = q.WriteOffset
		queue.roffset = q.ReadOffset
		queue.soffset = q.ScanOffset
		queue.num = q.Num
		queue.name = q.Name
		t.deadQueues[q.BindKey] = queue

		t.LogInfo(fmt.Sprintf("restore queue %s", queue.name))
	}
}

// 退出 Topic
func (t *Topic) exit() {
	defer t.LogInfo(fmt.Sprintf("topic.%s has exit.", t.name))

	t.ctx.Dispatcher.RemoveTopic(t.name)

	t.closed = true
	close(t.exitChan)
	t.wg.Wait()
}

// 消息推送
func (t *Topic) push(msg *Msg, routeKey string) error {
	queues := t.getQueuesByRouteKey(routeKey)
	if len(queues) == 0 {
		return fmt.Errorf("routeKey:%s is not match with queue, the mode is %d", routeKey, t.mode)
	}

	// 消息设置为延迟消息的话 将该消息推送到 topic.dispatcher 的 db 中，后续取用
	if msg.Delay > 0 {
		bindKeys := make([]string, len(queues))
		for i, q := range queues {
			bindKeys[i] = q.bindKey
		}
		msg.Expire = uint64(msg.Delay) + uint64(time.Now().Unix())
		return t.pushMsgToBucket(&DelayMsg{msg, bindKeys})
	}

	// 消息不是延迟消息 直接写到对应的队列中
	for _, q := range queues {
		if err := q.write(Encode(msg)); err != nil {
			return err
		}
		atomic.AddInt64(&t.pushNum, 1)
	}
	return nil
}

/* 推送机制的一些内置方法 */

// 生成bucket.key : delay + msgId
func creatBucketKey(msgId uint64, expire uint64) []byte {
	var buf = make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], expire)
	binary.BigEndian.PutUint64(buf[8:], msgId)
	return buf
}

// 解析bucket.key: delay + msgId
func parseBucketKey(key []byte) (uint64, uint64) {
	return binary.BigEndian.Uint64(key[:8]), binary.BigEndian.Uint64(key[8:])
}

// 延迟消息保存到bucket <delay+msgId>:<msg.Body(json)>
func (t *Topic) pushMsgToBucket(dg *DelayMsg) error {
	err := t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		bucket := tx.Bucket([]byte(t.name))
		key := creatBucketKey(dg.Msg.Id, dg.Msg.Expire)
		t.LogDebug(fmt.Sprintf("%v-%v-%v write in bucket", dg.Msg.Id, string(dg.Msg.Body), key))

		value, err := json.Marshal(dg)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, value); err != nil {
			return err
		}

		return nil
	})

	dg.Msg = nil
	dg = nil
	return err

}

// 添加消息到死信队列
func (t *Topic) pushMsgToDeadQueue(msg *Msg, bindKey string) error {
	queue := t.getDeadQueueByBindKey(bindKey, true)
	if err := queue.write(Encode(msg)); err != nil {
		return err
	}

	atomic.AddInt64(&t.deadNum, 1)
	return nil
}

// 检索延迟消息
func (t *Topic) retrievalBucketExpireMsg() error {
	if t.closed {
		err := errors.New(fmt.Sprintf("topic.%s has exit.", t.name))
		t.LogWarn(err)
		return err
	}

	var num int
	var err error

	err = t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		now := time.Now().Unix()
		bucket.ForEach(func(key, data []byte) error {
			delayTime, _ := parseBucketKey(key)
			if now < int64(delayTime) {
				return nil
			}

			var dg DelayMsg
			if err := json.Unmarshal(data, &dg); err != nil {
				t.LogError(fmt.Errorf("decode delay message failed, %s", err))
				goto deleteBucketElem
			}
			if dg.Msg.Id == 0 {
				t.LogError(fmt.Errorf("invalid delay message."))
				goto deleteBucketElem
			}

			for _, bindKey := range dg.BindKeys {
				queue := t.getQueueByBindKey(bindKey)
				if queue == nil {
					t.LogError(fmt.Sprintf("bindkey:%s is not associated with queue", bindKey))
					continue
				}
				if err := queue.write(Encode(dg.Msg)); err != nil {
					t.LogError(err)
					continue
				}
				atomic.AddInt64(&t.pushNum, 1)
				num++
			}
			return nil

		deleteBucketElem:
			if err := bucket.Delete(key); err != nil {
				t.LogError(err)
			}
			return nil
		})

		return nil
	})

	if err != nil {
		return err
	}
	if num == 0 {
		return ErrMessageNotExist
	}
	return nil
}

// 检测超时消息
func (t *Topic) retrievalQueueExipreMsg() error {
	if t.closed {
		err := fmt.Errorf("topic.%s has exit.", t.name)
		t.LogWarn(err)
		return err
	}

	num := 0
	for _, queue := range t.queues {
		for {
			// 扫描当前队列
			data, err := queue.scan()
			if err != nil {
				switch err {
				case ErrMessageNotExist:
				case ErrMessageNotExpire:
				default:
					t.LogError(err)
				}
				break
			}

			msg := Decode(data)
			if msg.Id == 0 {
				msg = nil
				break
			}

			if err := queue.removeWait(msg.Id); err != nil {
				t.LogError(err)
				break
			}

			msg.Retry++ // incr retry number
			if msg.Retry > uint16(t.msgRetry) {
				t.LogDebug(fmt.Sprintf("msg.Id %v has been added to dead queue.", msg.Id))
				if err := t.pushMsgToDeadQueue(msg, queue.bindKey); err != nil {
					t.LogError(err)
					break
				} else {
					continue
				}
			}

			// message is expired, and will be consumed again
			if err := queue.write(Encode(msg)); err != nil {
				t.LogError(err)
				break
			} else {
				t.LogDebug(fmt.Sprintf("msg.Id %v has expired and will be consumed again.", msg.Id))
				atomic.AddInt64(&t.pushNum, 1)
				num++
			}
		}
	}

	if num > 0 {
		return nil
	} else {
		return ErrMessageNotExist
	}
}

// 消费消息
func (t *Topic) pop(bindKey string) (*Msg, error) {
	queue := t.getQueueByBindKey(bindKey)
	if queue == nil {
		return nil, fmt.Errorf("bindKey:%s can't match queue", bindKey)
	}

	data := <-queue.readChan
	if data == nil {
		return nil, errors.New("no any message")
	}

	msg := Decode(data.data)
	if msg.Id == 0 {
		msg = nil
		return nil, errors.New("message decode failed.")
	}

	atomic.AddInt64(&t.popNum, 1)
	return msg, nil

}

// 死信队列消费
func (t *Topic) dead(bindKey string) (*Msg, error) {
	// 不新建地消费死信队列
	queue := t.getDeadQueueByBindKey(bindKey, false)
	if queue == nil {
		return nil, fmt.Errorf("bindkey:%s is not associated with queue", bindKey)
	}

	data, err := queue.read(t.isAutoAck)
	if err != nil {
		return nil, err
	}

	msg := Decode(data.data)
	if msg.Id == 0 {
		msg = nil
		return nil, errors.New("message decode failed.")
	}

	atomic.AddInt64(&t.deadNum, -1)
	return msg, nil
}

// 消息确认
func (t *Topic) ack(msgId uint64, bindKey string) error {
	queue := t.getQueueByBindKey(bindKey)
	if queue == nil {
		return fmt.Errorf("bindkey:%s is not associated with queue", bindKey)
	}

	return queue.ack(msgId)
}

// 设置topic消息
func (t *Topic) set(configure *topicConfigure) error {
	t.Lock()
	defer t.Unlock()

	// auto-ack
	if configure.isAutoAck == 1 {
		t.isAutoAck = true
	} else {
		t.isAutoAck = false
	}

	// route mode
	if configure.mode == ROUTE_KEY_MATCH_FULL {
		t.mode = ROUTE_KEY_MATCH_FULL
	} else if configure.mode == ROUTE_KEY_MATCH_FUZZY {
		t.mode = ROUTE_KEY_MATCH_FUZZY
	}

	// ttr
	if configure.msgTTR > 0 && configure.msgTTR < t.msgTTR {
		t.msgTTR = configure.msgTTR
	}

	// retry
	if configure.msgRetry > 0 && configure.msgRetry < t.msgRetry {
		t.msgRetry = configure.msgRetry
	}

	return nil
}

// 声明队列，绑定key必须是唯一值
// 队列名称为<topic_name>_<bind_key>
func (t *Topic) declareQueue(bindKey string) error {
	queue := t.getQueueByBindKey(bindKey)
	if queue != nil {
		return fmt.Errorf("bindKey %s has exist.", bindKey)
	}

	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	queueName := fmt.Sprintf("%s_%s", t.name, bindKey)
	t.queues[bindKey] = NewQueue(queueName, bindKey, t.ctx, t)
	return nil
}

// 根据路由键获取队列，支持全匹配和模糊匹配两种方式 使用正则实现
func (t *Topic) getQueuesByRouteKey(routeKey string) []*queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	// use default key when routeKey is empty
	if len(routeKey) == 0 {
		if q, ok := t.queues[DEFAULT_KEY]; ok {
			return []*queue{q}
		} else {
			queueName := t.generateQueueName(DEFAULT_KEY)
			t.queues[DEFAULT_KEY] = NewQueue(queueName, DEFAULT_KEY, t.ctx, t)
			return []*queue{t.queues[DEFAULT_KEY]}
		}
	}

	var queues []*queue
	for k, v := range t.queues {
		if t.mode == ROUTE_KEY_MATCH_FULL {
			if k == routeKey {
				queues = append(queues, v)
			}
		} else {
			if ok, _ := regexp.MatchString(routeKey, k); ok {
				queues = append(queues, v)
			}
		}
	}
	return queues
}

// 根据绑定键获取队列
func (t *Topic) getQueueByBindKey(bindKey string) *queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	if len(bindKey) == 0 {
		bindKey = DEFAULT_KEY
	}

	if q, ok := t.queues[bindKey]; ok {
		return q
	} else {
		return nil
	}
}

// 根据绑定键获取死信队列 没有的话 需要新建返回新建值 不需要新建返回nil
func (t *Topic) getDeadQueueByBindKey(bindKey string, createNotExist bool) *queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	if len(bindKey) == 0 {
		bindKey = DEFAULT_KEY
	}
	if q, ok := t.deadQueues[bindKey]; ok {
		return q
	}

	// 根据配置 新建死信队列
	if createNotExist {
		queueName := t.generateQueueName(fmt.Sprintf("%s_%s", bindKey, DEAD_QUEUE_FLAG))
		t.deadQueues[bindKey] = NewQueue(queueName, bindKey, t.ctx, t)
		return t.deadQueues[bindKey]
	}

	return nil
}

// 获取bucket堆积数量（延迟消息数量）
func (t *Topic) getBucketNum() int {
	var num int
	err := t.dispatcher.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		num = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		t.LogError(err)
	}

	return num
}

func (t *Topic) generateQueueName(bindKey string) string {
	return fmt.Sprintf("%s_%s", t.name, bindKey)
}

func (t *Topic) LogError(msg interface{}) {
	t.ctx.Logger.Error(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}

func (t *Topic) LogWarn(msg interface{}) {
	t.ctx.Logger.Warn(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}

func (t *Topic) LogInfo(msg interface{}) {
	t.ctx.Logger.Info(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}

func (t *Topic) LogDebug(msg interface{}) {
	t.ctx.Logger.Debug(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}

func (t *Topic) LogTrace(msg interface{}) {
	t.ctx.Logger.Trace(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}
