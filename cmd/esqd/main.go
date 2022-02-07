package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/judwhite/go-svc"

	"github.com/impact-eintr/enet/v2"
	"github.com/impact-eintr/esq"
	"github.com/impact-eintr/esq/mq"
)

type Router struct {
	*enet.BaseRouter
}

// TODO 无法在 Suber 退出的时候正常停止 亟待解决
func (this *Router) Handle(req enet.IRequest) {
	data := req.GetData()
	switch esq.ParseCommand(data) {
	case "PUB":
		esq.Pub(esq.ParseTopic(data), esq.ParseMsg(data), m, nil)

	case "SUB":
		topic, src := esq.ParseTopic(data), esq.ParseSrcHost(data)

		exitCh := this.Exit(req.GetConnection().GetConnID())
		esq.Sub(topic, src, m, func(msg []byte) {
			// 将收到的订阅消息发送给 消费者
			err := req.GetConnection().SendMsg(2020, msg)
			// 如果发送出现问题就结束订阅
			if err != nil {
				log.Println(err)
			}
		}, exitCh)

	case "UNSUB":
		esq.Unsub(esq.ParseTopic(data), esq.ParseSrcHost(data), m, nil)

	default:
		log.Println("invalid command type", esq.ParseCommand(data))
	}
}

var (
	m     = new(mq.Client)
	usage = `[NOTICE] If you need to enable debugging,
           please set the environment variable through "export esq_debug=true"`
)

type program struct {
	svr enet.IServer
}

func (p *program) Init(env svc.Environment) error {
	p.svr.AddRouter(0, &Router{enet.NewBaseRouter()})
	return nil
}

func (p *program) Start() error {
	go p.svr.Start()
	return nil
}

func (p *program) Stop() error {
	m.Close()
	p.svr.Stop()
	return nil
}

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
	fmt.Println(usage)

	prg := program{
		svr: enet.NewServer("tcp4"),
	}

	if err := svc.Run(&prg); err != nil {
		log.Fatal(err)
	}
}
