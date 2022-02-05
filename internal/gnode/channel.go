package gnode

import (
	"impact-eintr/esq/pkg/utils"
	"sync"
)

// TODO 一种抽象的通道
type Channel struct {
	key      string
	conns    map[*TcpConn]bool
	exitChan chan struct{}
	ctx      *Context
	wg       utils.WaitGroupWrapper
	sync.RWMutex
}
