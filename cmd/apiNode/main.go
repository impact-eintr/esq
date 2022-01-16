package main

import (
	"fmt"
	"net"
	"time"

	"github.com/impact-eintr/esq"
)

var (
	topic_heartbeat = "心跳"
	topic_filereq   = "文件定位请求"
)

//type LocateRouter struct {
//	enet.BaseRouter
//}
//
//func (this *LocateRouter) Handle(req iface.IRequest) {
//	log.Println("文件位于：", string(req.GetData()))
//}

func main() {
	//s := enet.NewServer("tcp4")
	//// 添加路由
	//s.AddRouter(20, &LocateRouter{})

	go func() {
		for {
			time.Sleep(time.Second)
			conn, err := net.Dial("tcp4", "127.0.0.1:6430")
			if err != nil {
				fmt.Println("client start err ", err)
				continue
			}
			defer conn.Close()

			nodeMap := make(map[string]time.Time, 0)

			//发封包message消息 只写一次
			msg := esq.PackageProtocol(0, "SUB", topic_heartbeat, "1.1.0.1", "我经常帮助一些翘家的人")
			_, err = conn.Write(msg)
			if err != nil {
				fmt.Println("write error err ", err)
				continue
			}

			// 不停地读
			for {
				data, err := esq.ReadOnce(conn)
				if err != nil {
					fmt.Println(err)
					break
				}
				nodeMap[string(data)] = time.Now()
				fmt.Println(nodeMap)
			}
		}

	}()
	select {}

	//go func() {
	//	localHost := "127.0.0.1:6431"
	//	time.Sleep(time.Second)
	//	conn, err := net.Dial("tcp4", "127.0.0.1:6430") // ESQ_SERVER
	//	if err != nil {
	//		fmt.Println("client start err ", err)
	//		return
	//	}
	//	defer conn.Close()

	//	for {
	//		time.Sleep(2 * time.Second)
	//		dp := enet.GetDataPack()
	//		msg := fmt.Sprintf("%s\t%s\n%s", topic_filereq, localHost, "涩图")
	//		data, _ := dp.Pack(enet.NewMsgPackage(0, []byte(msg)))
	//		//发封包message消息
	//		_, err = conn.Write(data)
	//		if err != nil {
	//			fmt.Println("write error err ", err)
	//			return
	//		}
	//	}
	//}()

	//s.Serve()
}
