package esqd

import (
	"net"
	"sync/atomic"
	"time"
)

const maxTimeout = time.Hour

type protocolV2 struct {
	ctx *context
}

// 因为这里是在Handler中启动的 所以这里其实是为每个客户端都启动了一个Loop
// 负责从客户端生产者接收消息 并最终发布给订阅者客户端
func (p *protocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	clientID := atomic.AddInt64(&p.ctx.esqd.clientIDCount, 1)
	client := newClientV2(clientID, conn, p.ctx)
	p.ctx.esqd.AddClient(client.ID, client)

	// messagePump 负责从channel中的memMsgCh和backend.ReadCh() 中读取消息
	// 并将消息推送给client
	messagePumpStartedCh := make(chan bool)
	go p.messagePump(client, messagePumpStartedCh)
	<-messagePumpStartedCh // 在消息准备好前 阻塞在此

	// 下面这个for循环负责接收客户端消息 比如消费订阅 以及生产消息等
	// 主要逻辑在p.Exec()中
	for {

		// 主要逻辑在这个Exec中
		response, err := p.Exec(clinet, params)

	}
}
