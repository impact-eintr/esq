package mq

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Broker interface {
	publish(topic string, msg []byte) error
	subscribe(topic string) (Interface, error)
	unsubscribe(topic string, sub Interface) error
	close()
	broadcast(msg []byte, subscribers []Interface)
}

// TODO  当前这个实现是基于磁盘的 后续把内存的队列再加回来
type BrokerImpl struct {
	exit     chan bool
	capacity int
	topics   map[string][]Interface
	ioChan   chan []byte
	sync.RWMutex

	// diskQueue config
	name            string
	dataPath        string
	maxBytesPerFile int64
	minMsgSize      int32
	maxMsgSize      int32
	syncEvery       int64
	syncTimeout     time.Duration
	logFunc         LogFunc
}

func NewBroker(dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration) *BrokerImpl {
	return &BrokerImpl{
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		minMsgSize:      minMsgSize,
		maxMsgSize:      maxMsgSize,
		syncEvery:       syncEvery,
		syncTimeout:     syncTimeout,
		logFunc: func(l LogLevel, f string, args ...interface{}) {
			if _, ok := os.LookupEnv("esq_debug"); ok {
				log.SetPrefix(fmt.Sprintf("%s\t", l.String()))
				log.Printf(f, args...)
				return
			} else {
				switch l {
				case DEBUG:
				case INFO:
				case WARN:
				default:
					log.SetPrefix(fmt.Sprintf("%s\t", l.String()))
					log.Printf(f, args...)
				}
			}
		},

		exit:   make(chan bool),
		topics: make(map[string][]Interface),
		ioChan: make(chan []byte, 1),
	}
}

// 进行消息的推送，有两个参数即topic、msg，分别是订阅的主题、要传递的消息
func (b *BrokerImpl) publish(topic string, pubmsg []byte) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()

	if !ok {
		return nil
	}

	b.broadcast(pubmsg, subscribers)
	return nil
}

// 消息的订阅，传入订阅的主题，即可完成订阅，并返回对应的channel通道用来接收数据
func (b *BrokerImpl) subscribe(topic string, src string) (Interface, error) {
	select {
	case <-b.exit:
		return nil, errors.New("broker closed")
	default:
	}

	queue := New(fmt.Sprintf("%s_%s", topic, src),
		b.dataPath, b.maxBytesPerFile,
		b.minMsgSize, b.maxMsgSize,
		b.syncEvery, time.Second,
		b.logFunc)

	b.Lock()
	b.topics[topic] = append(b.topics[topic], queue)
	b.Unlock()
	return queue, nil
}

// 取消订阅，传入订阅的主题和对应的通道
func (b *BrokerImpl) unsubscribe(topic string, sub Interface) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()
	if !ok {
		return nil
	}

	b.Lock()
	var newSubs []Interface
	for _, subscriber := range subscribers {
		if subscriber == sub {
			continue
		}
		newSubs = append(newSubs, subscriber)
	}
	b.Unlock()

	return nil

}

// 这个的作用就是很明显了，就是用来关闭消息队列的
func (b *BrokerImpl) close() {
	select {
	case <-b.exit:
		return
	default:
		close(b.exit)
		b.Lock()
		// 这里是否应该调用Interface.Close()
		for k := range b.topics {
			for i := range b.topics[k] {
				b.topics[k][i].Close()
			}
		}
		b.topics = make(map[string][]Interface)
		b.Unlock()
	}
}

// 这个属于内部方法，作用是进行广播，对推送的消息进行广播，保证每一个订阅者都可以收到
func (b *BrokerImpl) broadcast(msg []byte, subscribers []Interface) {
	count := len(subscribers)
	concurrency := 1

	switch {
	case count > 1000:
		concurrency = 3
	case count > 100:
		concurrency = 2
	default:
		concurrency = 1
	}

	pub := func(start int) {
		//采用Timer 而不是使用time.After 原因：time.After会产生内存泄漏 在计时器触发之前，垃圾回收器不会回收Timer
		idleDuration := 10 * time.Millisecond
		idleTimeout := time.NewTimer(idleDuration)
		defer idleTimeout.Stop()
		for j := start; j < count; j += concurrency {
			if !idleTimeout.Stop() {
				select {
				case <-idleTimeout.C:
				default:
				}
			}
			idleTimeout.Reset(idleDuration)
			select {
			case b.ioChan <- msg:
				subscribers[j].Put(<-b.ioChan)
			case <-idleTimeout.C:
			case <-b.exit:
				return
			}
		}
	}

	for i := 0; i < concurrency; i++ {
		go pub(i)
	}
}
