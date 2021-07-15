package esqd

import "sync"

// 消费者
type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

type Channel struct {
	requeueCount uint64 // 重新发送的消息数量
	messageCount uint64 // channel中的消息总数
	tieoutCount  uint64 // 发送超时的消息数量

	sync.RWMutex

	topicName string // topic名
	name      string // channel名
	ctx       *context

	backend   BackendQueue // 二级存储
	exitFlag  int32        // 准备退出
	exitMutex sync.RWMutex

	// stat tracking
	clients        map[int64]Consumer
	paused         int32
	temp           bool
	deleteCallback func(*Channel)
	delleter       sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// 下面都是由优先级队列实现的
	// 延迟消息投递，放入deferedPQ
	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex

	// 正在发送中的消息记录，直到收到客户端的FIN才会删除，否则timeout就会有重传的问题
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex
}
