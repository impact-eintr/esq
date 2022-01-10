package esq

import (
	"fmt"
	"log"

	"github.com/impact-eintr/esq/mq"
)

type Subfunc func(interface{}, chan bool)

// 订阅者一旦订阅就需要一直监听 因此需要提供一个退出的通知 然后使用回调函数消费消息
func Sub(topic string, mc *mq.Client, cb Subfunc) {
	ch, err := mc.Subscribe(topic)
	if err != nil {
		log.Println("subscribe failed")
		return
	}

	exitCh := make(chan bool, 1)
	for {
		select {
		case <-exitCh:
			fmt.Println("Sub 退出")
			return
		default:
			// 先看看消息是个啥
			val := mc.GetPayLoad(ch)
			fmt.Printf("get message is %s\n", val)
			// 有回调函数就调用
			if cb != nil {
				cb(val, exitCh)
			}
		}
	}
}
