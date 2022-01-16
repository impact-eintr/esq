package esq

import (
	"fmt"
	"log"
	"os"

	"github.com/impact-eintr/esq/mq"
)

type Subfunc func(interface{}, chan bool)

// 订阅者一旦订阅就需要一直监听 因此需要提供一个退出的通知 然后使用回调函数消费消息
func Sub(topic string, src string, mc *mq.Client, cb Subfunc) {
	ch, err := mc.Subscribe(topic, src)
	if err != nil {
		log.Println("subscribe failed")
		return
	}

	exitCh := make(chan bool, 1)
	for {
		select {
		case <-exitCh:
			if _, ok := os.LookupEnv("esq_debug"); ok {
				fmt.Println("subscribe exiting")
			}
			return
		default:
			// 先看看消息是个啥
			val := mc.GetPayLoad(ch)
			if _, ok := os.LookupEnv("esq_debug"); ok {
				fmt.Printf("get message is %s\n", val)
			}
			// 有回调函数就调用
			if cb != nil {
				cb(val, exitCh)
			}
		}
	}
}
