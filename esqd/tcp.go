package esqd

import "net"

type tcpServer struct {
	ctx *context
}

func (p *tcpServer) Handle(clientConn net.Conn) {

}
