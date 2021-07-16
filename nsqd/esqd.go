package esqd

// esqd 的逻辑
// Producer --> 消息 --> topic --> channels --> Consumer

import (
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/impact-eintr/esq/internal/dirlock"
	"github.com/impact-eintr/esq/internal/http_api"
	"github.com/impact-eintr/esq/internal/util"
)

type errStore struct {
	err error
}

type Client interface {
	Stats() ClientStats
	IsProducer() bool
}

type ESQD struct {
	clientIDCount int64 // 连接esqd 的client 数量

	sync.RWMutex

	opts atomic.Value // esqd配置文件

	dl        *dirlock.DirLock // 文件锁
	isLoading int32
	errValue  atomic.Value
	startTime time.Time

	// esqd中 topic map
	topicMap map[string]*Topic

	clientLock  sync.RWMutex
	clients     map[int64]Client
	lookupPeers atomic.Value

	tcpListener  net.Listener
	httpListener net.Listener

	poolSize int

	notifyCh           chan interface{} // 修改topic/chan的时候通知lookup
	optsNotificationCh chan struct{}
	exitCh             chan int
	waitGroup          util.WaitGroupWrapper

	// ci *clusterinfo.ClusterInfo
}

// esqd元数据结构体 nsqd进程退出时写入磁盘 启动时读取
type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"` // topic状态
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json::paused` // channel状态
		} `json:"channels"`
	} `json:"topics"`
}

func New(opts *Options) (*ESQD, error) {
	var err error

	dataPath := opts.DataPath
	if opts.DataPath == "" {
		pwd, _ := os.Getwd()
		dataPath = pwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix,
			log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	e := &ESQD{
		startTime:          time.Now(),
		topicMap:           make(map[string]*Topic),
		clients:            make(map[int64]Client),
		exitCh:             make(chan int),
		notifyCh:           make(chan interface{}),
		optsNotificationCh: make(chan struct{}, 1),
		dl:                 dirlock.New(dataPath),
	}

	// http 客户端
	httpcli := http_api.NewClient(opts.HTTPClientConnTimeout, opts.HTTPClientReqTimeout)
	// 配置集群New
	// e.ci = clusterinfo.New(n.logf, httpcli)

	e.lookupPeers.Store([]*lookupPeer{})

	// tcp 客户端

	return e, err

}

func (e *ESQD) Main() error {
	ctx := &context{e}

	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				e.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// 确保所有的gouroutine都运行完毕
	// 建立tcpSerrver
	tcpServer := &serverServer{ctx: ctx}
	e.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(e.tcpListener, tcpServer, e.logf))
	})
	// 建立httpSerrver
	httpServer := newHTTPServer(ctx)
	e.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(e.httpListener, httpServer, "HTTP", e.logf))
	})

	// 队列scan扫描协程
	e.waitGroup.Wrap(e.queueScanLoop)
	// lookup查找协程
	e.waitGroup.Wrap(e.lookupLoop)
	if e.getOpts().StatsAddress != "" {
		e.waitGroup.Wrap(e.statsdLoop)
	}

	err := <-exitCh // 阻塞在这里
	return err
}

func (e *ESQD) getOpts() *Options {
	return e.opts.Load().(*Options)

}

func (e *ESQD) swapOpts(opts *Options) {
	e.opts.Store(opts)
}

// 通知lookuploop topic和channel的更改
func (e *ESQD) Notify(v interface{}) {
	perrsist := atomic.LoadInt32(&e.isLoading) == 0
	e.waitGroup.Wrap(func() {
		select {
		case <-e.exitCh:
		case e.notifyCh <- v:
			if !perrsist {
				return
			}
			e.Lock()
			err := e.PersistMetadata()
			if err != nill {
				e.logf(LOG_ERROR, "faild to persist metadata - %s", err)
			}
			e.Unlock()
		}
	})

}

func (e *ESQD) PersistMetadata() error {

}
