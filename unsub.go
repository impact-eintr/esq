package esq

import (
	"fmt"

	"github.com/impact-eintr/esq/mq"
)

type Unsubfunc func()

// 订阅者一旦订阅就需要一直监听 因此需要提供一个退出的通知 然后使用回调函数消费消息
func Unsub(topic, src string, mc *mq.Client, cb Unsubfunc) {
	subname := fmt.Sprintf("%s_%s", topic, src)
	err := mc.Unsubscribe(topic, subname)
	if err != nil {
		return
	}
	if cb != nil {
		cb()
	}
}
