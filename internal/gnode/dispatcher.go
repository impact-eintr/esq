package gnode

import (
	"fmt"
	"impact-eintr/esq/pkg/logs"
	"impact-eintr/esq/pkg/utils"
	"sync"

	"github.com/impact-eintr/bolt"
)

// dispatcher 调度器,负责管理topic
type Dispatcher struct {
	ctx       *Context
	db        *bolt.DB
	wg        utils.WaitGroupWrapper
	closed    bool
	poolSize  int
	snowflake *utils.Snowflake // 雪花码
	exitChan  chan struct{}

	// topic
	topics   map[string]*Topic
	topicMux sync.RWMutex // topics 的锁

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
