package esq

import (
	"log"

	"github.com/impact-eintr/esq/mq"
)

type Pubfunc func(interface{})

func Pub(topic string, pubmsg string, mc *mq.Client, cb Pubfunc) {
	// 发布消息
	err := mc.Publish(topic, []byte(pubmsg))
	if err != nil {
		log.Println("publish message failed")
	}
	// 调用回调函数
	if cb != nil {
		cb(nil)
	}
}
