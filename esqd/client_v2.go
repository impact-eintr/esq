package esqd

import (
	"net"
)

const defaultBufferSize = 16 * 1024

// 当前client的状态
const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed // 订阅
	stateClosing
)

func newClientV2(id int64, conn net.Conn, ctx *context) {
}
