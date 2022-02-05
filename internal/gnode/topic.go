package gnode

import (
	"impact-eintr/esq/pkg/utils"
	"sync"
	"time"
)

// topic 消息主题,即消息类型,每一条消息都有所属topic,topic会维护多个queue
type Topic struct {
	name      string
	mode      int
	msgTTR    int  // TODO TTP
	msgRetry  int  // TODO 重试次数
	isAutoAck bool // TODO 是否配置成自动回复

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
