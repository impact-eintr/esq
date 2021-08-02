package esqd

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/impact-eintr/esq/diskqueue"
	"github.com/impact-eintr/esq/internal/lg"
	"github.com/impact-eintr/esq/internal/util"
)

// Topic 订阅主题
// 生产者发布的逻辑key
type Topic struct {
	msgCount uint64 // topic中的消息数量
	msgBytes uint64 // topic中消息的字节量

	sync.RWMutex

	name       string              // Topic的名字
	channelMap map[string]*Channel // Topic对应的channel map
	backend    BackendQueue        // 二级磁盘存储队列
	memMsgChan chan *Message       // 消息队列当nsqd收到消息则写入

	startCh         chan int              // 启动channel
	exitCh          chan int              // 终止channel
	channelUpdateCh chan int              // topic对应的channel map 发生改变更新
	waitGroup       util.WaitGroupWrapper // 多个时间等待准备退出
	exitFlag        uint32                // topic是否准备退出
	idFactory       *guidFactory          // id生成工厂

	tempFlag       bool         // 是否临时 临时的topic不存入磁盘
	deleteCallback func(*Topic) // 删除callback
	deleter        sync.Once    // 保证只执行一次

	paused  int32    // topic是否暂停
	pauseCh chan int // pause回调

	ctx *context //上下文
}

func NewTopic(topicName string, ctx *context, deleteCaback func(*Topic)) *Topic {
	t := &Topic{
		name:            topicName,
		channelMap:      make(map[string]*Channel),
		memMsgChan:      make(chan *Message, ctx.esqd.getOpts().MemQueueSize),
		startCh:         make(chan int, 1),
		exitCh:          make(chan int),
		channelUpdateCh: make(chan int),
		ctx:             ctx,
		paused:          0,
		pauseCh:         make(chan int),
		deleteCallback:  deleteCaback,
		idFactory:       NewGUIDFactory(ctx.esqd.getOpts().ID),
	}

	if strings.HasPrefix(topicName, "#temp") {
		t.tempFlag = true
		t.backend = newDummyBackendQueue() // 临时队列不会落盘
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.esqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, opts.LogLevel, f, args...)
		}

		// diskqueue 里面会维护一个读写文件的ioLoop
		// 这里面会接受一条消息并写到文件或者从文件中读取一条消息并投送出去
		t.backend = diskqueue.New(
			topicName,
			ctx.esqd.getOpts().DataPath,
			ctx.esqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgSize),
			int32(ctx.esqd.getOpts().MaxMsgSize)+minValidMsgSize,
			ctx.esqd.getOpts().SyncEvery,
			ctx.esqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	t.waitGroup.Wrap(t.messagePump)

	// 通知lookup有新的topic
	t.ctx.esqd.Notify(t)

	return t

}

// 内存消息落盘
//func (t *Topic) putMemoryToFile() {
//	message := t.memMsgChan
//	// 阻塞落盘
//	b := bufferrPoolGget()
//	_ = writeMessaggeToBackendd(b, messageg, t.memBackend)
//	bufferPoolPut(b)
//}

func (t *Topic) Start() {
	select {
	case t.startCh <- 1:
	default:
	}
}

func (t *Topic) Exiting() bool {
	return atomic.LoadUint32(&t.exitFlag) == 1
}

/* 重要函数*/
/*不断从memMsgCh 和 backemd 队列中读取， 并将每个消息都复制一次，*/
/*发送给topic 下的所有channel(channel 会修改消息中的字段)*/
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memMsgCh chan *Message
	var backendCh chan []byte

	// 这里要等到startCh完成后才能往下走
	for {
		select {
		case <-t.channelUpdateCh:
			continue
		case <-t.pauseCh:
			continue
		case <-t.exitCh:
			goto exit
		case <-t.startCh:
			// 也就是要等到topic执行完getChannel()后才会接着往下走
		}
		break
	}

	t.RLock()
	// 将所有channel通道放到chans中
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	if len(chans) > 0 && !t.IsPaused() {
		memMsgCh = t.memMsgChan
		backendCh = t.backend.ReadChan()
	}

	// 主要消息loop
	for {
		select { // 使用select 监听两个chan是否有消息传入
		// 消息已经被写入磁盘的话 nsq的消费消息就是无序的 因为select的选择就是无序的
		case msg = <-memMsgCh:
			// 如果topic有收到消息
		case buf = <-backendCh:
			// 而如果消息是从dickqueue中传过来的 还要解码反序列化成msg
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.esqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateCh: // 当channel信息更新后 重新设置chan
			// 如果有新的channel
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memMsgCh = nil
				backendCh = nil
			} else {
				memMsgCh = t.memMsgChan
				backendCh = t.backend.ReadChan()
			}
			continue
		case <-t.pauseCh: // 暂停信号
			if len(chans) == 0 || t.IsPaused() {
				memMsgCh = nil
				backendCh = nil
			} else {
				memMsgCh = t.memMsgChan
				backendCh = t.backend.ReadChan()
			}
			continue
		case <-t.exitCh: // 退出信号
			goto exit
		}

		// 遍历每一个channel通道 将消息投送出去
		for i, channel := range chans {
			chanMsg := msg // 复制消息

			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.TimeStamp = msg.TimeStamp
				chanMsg.deferred = msg.deferred
			}

			if chanMsg.deferred != 0 {
				// 如果是延时消息则将延时消息丢给延时channel
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			// 否则正常提交消息
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.ctx.esqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.ctx.esqd.logf(LOG_INFO, "TOPIC(%s) closeing ... messagePump", t.name)
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}
