package esq

import "github.com/impact-eintr/esq/mq"

func Close(mc *mq.Client) {
	mc.Close()
}
