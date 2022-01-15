package main

import (
	"flag"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/impact-eintr/enet"
	"github.com/impact-eintr/enet/iface"
	"github.com/impact-eintr/esq"
	"github.com/impact-eintr/esq/mq"
)

type PubRouter struct {
	enet.BaseRouter
}

func (this *PubRouter) Handle(req iface.IRequest) {
	msgs := strings.Split(string(req.GetData()), "\t") // topic\tmessage
	esq.Pub(msgs[0], msgs[1], m, nil)
}

type SubRouter struct {
	enet.BaseRouter
}

func (this *SubRouter) Handle(req iface.IRequest) {
	msgs := strings.Split(string(req.GetData()), "\t") // topic\tmessage
	go esq.Sub(msgs[0], m, func(v interface{}, ch chan bool) {
		err := req.GetConnection().SendTcpMsg(2020, v.([]byte))
		if err != nil {
			ch <- true
		}
	})
}

var m = new(mq.Client)

func init() {
	path := flag.String("path", "/tmp/esq", "queue path")
	filesize := flag.Int64("filesize", 65536, "file size")
	minsize := flag.Int("minsize", 0, "min msg size")
	maxsize := flag.Int("maxsize", math.MaxInt32, "max msg size")
	sync := flag.Int64("sync", 1024, "sync count")

	flag.Parse()

	m = mq.NewClient(
		*path,
		*filesize,
		int32(*minsize),
		int32(*maxsize),
		*sync,
		time.Second, // 同步计时
	)
}

func main() {
	fmt.Println("[NOTICE] If you need to enable debugging,please set the environment variable through `export esq_debug`")
	s := enet.NewServer("tcp4")

	s.AddRouter(0, &PubRouter{})  // Publish
	s.AddRouter(10, &SubRouter{}) // Subscribe

	s.Serve()
}
