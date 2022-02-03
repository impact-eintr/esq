package esq

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/impact-eintr/esq/mq"
)

type Subscriber struct {
	exitCh chan bool
	queue  mq.Interface
	sync.Once
}

func NewSubscriber() *Subscriber {
	return &Subscriber{exitCh: make(chan bool)}
}

type Subfunc func([]byte)

// 订阅者一旦订阅就需要一直监听 因此需要提供一个退出的通知 然后使用回调函数消费消息
func (s *Subscriber) Sub(topic string, src string, mc *mq.Client, cb Subfunc) {
	go func() {
		var err error
		s.queue, err = mc.Subscribe(topic, src)
		if err != nil {
			log.Println("subscribe failed")
			return
		}

		for {
			select {
			case <-s.exitCh:
				// 只取消不删除
				mc.HandleSubscribeError(topic, s.queue)
				fmt.Println("subscribe exiting")
				return
			default:
				// 先看看消息是个啥
				val := mc.GetPayLoad(s.queue)
				if _, ok := os.LookupEnv("esq_debug"); ok {
					fmt.Printf("get message is %s\n", val)
				}
				// 有回调函数就调用
				if cb != nil {
					go cb(val)
				}
			}
		}
	}()
}

func (s *Subscriber) Exit() {
	s.Do(func() {
		close(s.exitCh)
	})
}
