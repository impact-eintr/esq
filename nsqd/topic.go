package esqd

import (
	"strings"
	"sync"

	"github.com/impact-eintr/esq/internal/lg"
	"github.com/impact-eintr/esq/internal/util"
	"github.com/nsqio/go-diskqueue"
)

// 生产者发布的逻辑key
type Topic struct {
	msgCount uint64 // topic中的消息数量
	msgBytes uint64 // topic中消息的字节量

	sync.RWMutex

	name       string              // Topic的名字
	chanelMap  map[string]*Channel // Topic对应的channel map
	backend    BackendQueue        // 二级磁盘存储队列
	memMsgChan chan *Message       // 消息队列当nsqd收到消息则写入

	memToFileCh chan *Message // 将内存中的消息定时刷新到磁盘中
	memBackend  BackendQueue  // 内存消息对应的磁盘存储队列

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
		chanelMap:       make(map[string]*Channel),
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
		t.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LoLevel, f string, args ...interface{}) {
			opts := ctx.esqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, opts.LogLevel, f, args...)
		}

		t.memBackend = diskqueue.New()

		t.waitGroup.Wrap(t.mmessaePump)

		// 内存消息落盘
		t.waitGroup.Wrap(t.putMemoryToFile)

		// 通知lookup有新的topic
		t.ctx.esqd.Notify(t)

		return t
	}

}
