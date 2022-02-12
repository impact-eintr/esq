package gnode

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/impact-eintr/bolt"
	"github.com/impact-eintr/esq/pkg/logs"
	"github.com/impact-eintr/esq/pkg/utils"
)

// dispatcher 调度器,负责管理topic
type Dispatcher struct {
	ctx       *Context
	db        *bolt.DB
	wg        utils.WaitGroupWrapper
	closed    bool
	poolSize  int64
	snowflake *utils.Snowflake // 雪花码
	exitChan  chan struct{}

	// topic
	topics   map[string]*Topic // 当前 Dispatcher 拥有的所有 topic
	topicMux sync.RWMutex      // topics 的锁

	// channel
	channels   map[string]*Channel
	channelMux sync.RWMutex // channels 的锁
}

// 新建并初始化 调度器
func NewDispatcher(ctx *Context) *Dispatcher {
	// 申请一个 snowflake 对象 用来产生消息ID
	sn, err := utils.NewSnowflake(int64(ctx.Conf.NodeId))
	if err != nil {
		panic(err)
	}

	// 建立 当前调度器的持久化机制
	dbFile := fmt.Sprintf("%s/gmq.db", ctx.Conf.DataSavePath)
	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		panic(err)
	}

	dispatcher := &Dispatcher{
		db:        db,
		ctx:       ctx,
		snowflake: sn,
		topics:    make(map[string]*Topic),
		channels:  make(map[string]*Channel),
		exitChan:  make(chan struct{}),
	}

	ctx.Dispatcher = dispatcher
	return dispatcher
}

// 调度器的阻塞运行 直到收到来自 Gnode 的退出通知 调用 d.exit() 通知所有子goroutine退出
func (d *Dispatcher) Run() {
	defer d.LogInfo("dispatcher exit.")
	d.wg.Wrap(d.scanLoop)

	select {
	case <-d.ctx.Gnode.exitChan:
		d.exit()
	}
}

// 通知 Dispatcher 下的所有goroutine 退出
func (d *Dispatcher) exit() {
	d.closed = true
	_ = d.db.Close()
	close(d.exitChan) // 发出通知

	for _, t := range d.topics {
		t.exit()
	}
	for _, c := range d.channels {
		c.exit()
	}

	d.wg.Wait()
}

// 定时扫描各个topic.queue延迟消息
// 由dispatcher统一扫描,可以避免每个topic都需要建立定时器的情况
// 每个topic都建立定时器,会消耗更多的cpu
func (d *Dispatcher) scanLoop() {
	selectNum := 20                                // 每次检索20个topic的延迟消息
	workCh := make(chan *Topic, selectNum)         // 用于分发topic给worker处理
	responseCh := make(chan bool, selectNum)       // 用于worker处理完任务后响应
	closeCh := make(chan int)                      // 用于通知worker退出
	scanTicker := time.NewTicker(1 * time.Second)  // 定时扫描时间
	freshTicker := time.NewTicker(3 * time.Second) // 刷新topic集合

	topics := d.GetTopics() // 获取当前 dispatcher 的所有 Topic
	d.resizePool(len(topics), workCh, closeCh, responseCh)

	for {
		// 每次检索20个topic的延迟消息
		// 此值不超过topic的总数
		selectNum = 20

		select {
		case <-d.exitChan:
			goto exit
		case <-scanTicker.C:
			if len(topics) == 0 {
				continue
			}
		case <-freshTicker.C:
			topics = d.GetTopics()
			d.resizePool(len(topics), workCh, closeCh, responseCh)
			continue
		}

		if selectNum > len(topics) {
			selectNum = len(topics)
		}
	loop:
		if d.closed {
			goto exit
		}

		// 从topic集合中随机挑选selectNum个topic给worker处理
		// worker数量不够多的时候,这里会阻塞
		for _, i := range utils.UniqRands(selectNum, len(topics)) {
			workCh <- topics[i]
		}

		// 记录worker能够正常处理的topic个数
		hasMsgNum := 0
		for i := 0; i < selectNum; i++ {
			if <-responseCh {
				hasMsgNum++
			}
		}

		// 如果已处理个数超过选择topic个数的四分之一,说明这批topic还是有很大几率有消息的
		// 继续重新随机挑选topic处理,否则进入下一个定时器
		// TODO: 这个算法的规则是优先执行选中topic消息,直到没有消息后,才会去进行下轮遍历,
		// 所以若被选中的topic一直有消息产生,则会导致其他新加入topic的消息无法被即时处理
		// 如果你的业务是很频繁推送多个topic,并且是延迟消息,可以尽量调大selectNum的值
		if float64(hasMsgNum)/float64(selectNum) > 0.25 {
			log.Println("goto loop")
			goto loop
		}
	}
exit:
	// close(closeCh)通知所有worker退出
	close(closeCh)
	scanTicker.Stop()
	freshTicker.Stop()

}

// worker 负责处理延迟消息(无法立刻处理的消息)
func (d *Dispatcher) scanWorker(workCh chan *Topic, closeCh chan int, responseCh chan bool) {
	for {
		select {
		case <-closeCh:
			atomic.AddInt64(&d.poolSize, -1)
			return
		case topic := <-workCh:
			err := topic.retrievalBucketExpireMsg()
			err2 := topic.retrievalQueueExipreMsg()
			if err != nil && err2 != nil {
				responseCh <- false
			} else {
				responseCh <- true
			}
		}
	}
}

// 调整(+/-) Dispatcher 的工作线程数量
func (d *Dispatcher) resizePool(topicNum int, workCh chan *Topic, closeCh chan int, responseCh chan bool) {
	// 每次处理 1/4 数量的topic
	workerNum := int64(float64(topicNum) * 0.25)
	if workerNum < 1 {
		workerNum = 1
	}

	// topic数量增加 导致需要创建更多的worker数量 启动行的worker并增加池的大小
	if workerNum > atomic.LoadInt64(&d.poolSize) {
		d.LogInfo(fmt.Sprintf("start %v of workers to scan delay message", workerNum))
		d.wg.Wrap(func() {
			d.scanWorker(workCh, closeCh, responseCh)
		})
		atomic.AddInt64(&d.poolSize, 1)
		return
	}

	// 当需要的woker数量小于池大小时,发送关闭信号到closeCh管道
	// worker从closeCh管道收到消息后,关闭退出
	// TODO 目前topic是不会自己消失的,所以需要一个机制,当topic没有客户端连接时,关闭topic
	if workerNum < d.poolSize {
		d.LogInfo("reduce scan-Worker for pool", workerNum, d.poolSize)
		for i := d.poolSize - workerNum; i > 0; i-- {
			closeCh <- 1
		}
	}
}

// get topic by name
// create a new topic if it is not exist
func (d *Dispatcher) GetTopic(name string) *Topic {
	d.topicMux.RLock()
	if t, ok := d.topics[name]; ok {
		d.topicMux.RUnlock()
		return t
	} else {
		d.topicMux.RUnlock()

		d.topicMux.Lock()
		defer d.topicMux.Unlock()
		t := NewTopic(name, d.ctx)
		d.topics[name] = t
		return t
	}
}

// get topic
// returns error when it is not exist
func (d *Dispatcher) GetExistTopic(name string) (*Topic, error) {
	d.topicMux.RLock()
	defer d.topicMux.RUnlock()

	if t, ok := d.topics[name]; ok {
		return t, nil
	} else {
		return nil, errors.New("topic is not exist")
	}
}

// get all topics
func (d *Dispatcher) GetTopics() []*Topic {
	d.topicMux.RLock()
	defer d.topicMux.RUnlock()

	var topics []*Topic
	for _, t := range d.topics {
		topics = append(topics, t)
	}
	return topics
}

// remove topic by topic.name
func (d *Dispatcher) RemoveTopic(name string) {
	d.topicMux.Lock()
	if t, ok := d.topics[name]; ok {
		delete(d.topics, t.name)
	}

	d.topicMux.Unlock()
}

// get channel
// create channel if is not exist
func (d *Dispatcher) GetChannel(key string) *Channel {
	d.channelMux.RLock()
	if c, ok := d.channels[key]; ok {
		d.channelMux.RUnlock()
		return c
	} else {
		d.channelMux.RUnlock()
		d.channelMux.Lock()
		c := NewChannel(key, d.ctx)
		d.channels[key] = c
		d.channelMux.Unlock()
		return c
	}
}

// remove channel by channel.key
func (d *Dispatcher) RemoveChannel(key string) {
	d.channelMux.Lock()
	if c, ok := d.channels[key]; ok {
		delete(d.channels, c.key)
	}

	d.channelMux.Unlock()
}

// 消息推送
// 每一条消息都需要dispatcher统一分配msg.Id
func (d *Dispatcher) push(name string, routeKey string, data []byte, delay int) (uint64, error) {
	msgId := d.snowflake.Generate()
	msg := &Msg{}
	msg.Id = msgId
	msg.Delay = uint32(delay)
	msg.Body = data

	topic := d.GetTopic(name)
	err := topic.push(msg, routeKey)
	msg = nil

	return msgId, err
}

// consume message
func (d *Dispatcher) pop(name, bindKey string) (*Msg, error) {
	topic := d.GetTopic(name)
	return topic.pop(bindKey)
}

// consume dead message
func (d *Dispatcher) dead(name, bindKey string) (*Msg, error) {
	topic := d.GetTopic(name)
	return topic.dead(bindKey)
}

// ack message
func (d *Dispatcher) ack(name string, msgId uint64, bindKey string) error {
	topic := d.GetTopic(name)
	return topic.ack(msgId, bindKey)
}

// config
func (d *Dispatcher) set(name string, configure *topicConfigure) error {
	topic := d.GetTopic(name)
	return topic.set(configure)
}

// declare queue
func (d *Dispatcher) declareQueue(queueName, bindKey string) error {
	topic := d.GetTopic(queueName)
	return topic.declareQueue(bindKey)
}

// subscribe channel
func (d *Dispatcher) subscribe(channelName string, conn *TcpConn) error {
	channel := d.GetChannel(channelName)
	return channel.addConn(conn)
}

func (d *Dispatcher) publish(channelName string, msg []byte) error {
	channel := d.GetChannel(channelName)
	return channel.publish(msg)
}

func (d *Dispatcher) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Dispatcher"))
	v = append(v, msg...)
	d.ctx.Logger.Error(v...)
}

func (d *Dispatcher) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Dispatcher"))
	v = append(v, msg...)
	d.ctx.Logger.Warn(v...)
}

func (d *Dispatcher) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Dispatcher"))
	v = append(v, msg...)
	d.ctx.Logger.Info(v...)
}
