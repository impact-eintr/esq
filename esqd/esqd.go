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

	clientLock sync.RWMutex
	clients    map[int64]Client

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
	tcpServer := &tcpServer{ctx: ctx}
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

func (e *ESQD) queueScanLoop() {
	workCh := make(chan *Channel, e.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, e.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	// 定时执行loop的间隔时间，默认100ms
	workTicker := time.NewTicker(e.getOpts().QueueScanInterval)
	// 刷新channel数量，重新调整协程池，默认时间是5s刷新一次
	refreshTicker := time.NewTicker(e.getOpts().QueueScanRefreshInterval)

	channels := e.channels()
	e.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			// 定时执行loop的间隔时间，默认100ms
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			// 刷新channel数量，重新调整协程池，当协程池容量足够是会开启新的goroutine
			channels = e.channels()
			e.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-e.exitCh:
			goto exit
		}

		num := e.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		//从 channels 中随机选择 num 个 channel
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		//等待处理响应，记录失败次数
		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		//queueScanLoop的处理方法模仿了Redis的概率到期算法
		//(probabilistic expiration algorithm)，
		//每过一个QueueScanInterval(默认100ms)间隔，进行一次概率选择，
		//从所有的channel缓存中随机选择QueueScanSelectionCount(默认20)个channel，
		//如果某个被选中channel存在InFlighting消息或者Deferred消息，
		//则认为该channel为“脏”channel。
		//如果被选中channel中“脏”channel的比例大于QueueScanDirtyPercent(默认25%)，
		//则不投入睡眠，直接进行下一次概率选择
		if float64(numDirty)/float64(num) > e.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	e.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func (e *ESQD) channels() []*Channel {
	var channels []*Channel
	e.RLock()
	for _, t := range e.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	e.RUnlock()
	return channels
}

// 协程池调整
func (e *ESQD) resizePool(num int, workCh chan *channel,
	responseCh chan bool, closeCh chan int) {
	// 协程池大小 = 总channel数 / 4
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > e.getOpts().QueueScanWorkerPoolMax {
		// idealPoolSize 协程池大小 最大默认是4个
		idealPoolSize = e.getOpts().QueueScanWorkerPoolMax
	}

	for {
		if idealPoolSize == e.poolSize {
			break
		} else if idealPoolSize < e.poolSize {
			// 协程池协程容量 < 当前已经开启的协程数量
			// 说明开启的协程过多 需要关闭协程
			// closeCh queueScanWorker 会中断 "守护"协程
			// 关闭后 将当前开启的协程数量-1
			closeCh <- 1
			e.poolSize--
		} else {
			e.waitGroup.Wrap(func() {
				e.queueScanWorker(workCh, responseCh, closeCh)
			})
			e.poolSize++
		}
	}
}

func (e *ESQD) queueScanWorker(workCh chan *Channel,
	responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			// 处理一次某个channel的消息
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			// 如果这个
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
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
