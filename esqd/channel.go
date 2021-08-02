package esqd

import (
	"sync"
	"sync/atomic"

	"github.com/impact-eintr/esq/internal/lg"
)

// 消费者
type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// topic 在 messagePump()中处理消息的时候，
// 通过下面两个函数，将消息投递到 channel。
// 延时消息
// channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
// 非延时消息
// channel.PutMessage(chanMsg)

type Channel struct {
	requeueCount uint64 // 重新发送的消息数量
	messageCount uint64 // channel中的消息总数
	timeoutCount  uint64 // 发送超时的消息数量

	sync.RWMutex

	topicName string // topic名
	name      string // channel名
	ctx       *context

	backend BackendQueue // 磁盘队列

	memMsgCh chan *Message // channel的消息管道
	// 这是存放消息的内存 默认配置10000的长度 超过后就会落盘
	exitFlag  int32 // 准备退出
	exitMutex sync.RWMutex

	// stat tracking
	clients        map[int64]Consumer // 关联的是客户端的订阅者
	paused         int32
	temp           bool
	deleteCallback func(*Channel)
	delleter       sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// 延迟消息存放的地方，其中 deferredPQ，是一个优先级管理的队列，直接丢在内存
	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex

	// 正在发送中的消息记录，直到收到客户端的FIN才会删除，否则timeout就会有重传的问题
	// 这个标识正在执行中的消息，也是直接丢在内存
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
}

func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := Channel{
		topicName:      topicName,
		name:           channelName,
		memMsgCh:       make(chan *Message, ctx.esqd.getOpts().MemQueueSize),
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}

}

func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShitf(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		// 只要有消息这个通道就是脏的
		dirty = true
		_, err : c.popInFlightMseeage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		// 循环将channel中存在inFlightPQ中的消息put出去
		// put 到memMsgCh 或 磁盘
		c.put(msg)
	}

exit:
	return dirty
}


func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		//区别在这个PeekAndShift里面
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		//put到memoryMsgChan或者磁盘
		c.put(msg)
	}

exit:
	return dirty
}
func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgCh <- m:
		//将消息写入到memoryMsgChan中去
	default:
		//如果memoryMsgChan满了则将消息写到磁盘中区
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPollPut(b)
		c.ctx.esqd.SetHealth(err)
		if err != nil {
			c.ctx.esqd.logf(lg.ERROR, "CHANNEL[%s]: failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

func (c *Channel) Exiting() bool {

}
