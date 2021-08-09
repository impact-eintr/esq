package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"

	"github.com/impact-eintr/esq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}

// 这个TCPServer函数是公共函数部分
// 因此这个函数也用于esqd的tcp服务 这个函数和平时见到的go网络编程模型一样
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.LogFunc) error {
	logf(lg.INFO, "[TCP]: listening on %s", listener.Addr())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}

			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}
		go handler.Handle(clientConn)
	}
	logf(lg.INFO, "[TCP]: closing %s", listener.Addr())

	return nil

}
