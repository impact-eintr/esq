package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/impact-eintr/enet"
	"github.com/impact-eintr/enet/iface"
)

var (
	topic_heartbeat = "心跳"
	topic_filereq   = "文件定位请求"
)

type LocateRouter struct {
	enet.BaseRouter
}

func (this *LocateRouter) Handle(req iface.IRequest) {
	log.Println("文件位于：", string(req.GetData()))
}

func main() {
	s := enet.NewServer("tcp4")
	// 添加路由
	s.AddRouter(20, &LocateRouter{})

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
			dp := enet.GetDataPack()
			msg, _ := dp.Pack(enet.NewMsgPackage(10, []byte(topic_heartbeat+"\t"+"我经常帮助一些翘家的人")))
			_, err = conn.Write(msg)
			if err != nil {
				fmt.Println("write error err ", err)
				continue
			}

			// 不停地读
			for {
				//先读出流中的head部分
				headData := make([]byte, dp.GetHeadLen())
				_, err = io.ReadFull(conn, headData) //ReadFull 会把msg填充满为止
				if err != nil {
					fmt.Println("read head error")
					break
				}
				//将headData字节流 拆包到msg中
				msgHead, err := dp.Unpack(headData)
				if err != nil {
					fmt.Println("server unpack err:", err)
					break
				}

				if msgHead.GetDataLen() > 0 {
					//msg 是有data数据的，需要再次读取data数据
					msg := msgHead.(*enet.Message)
					msg.Data = make([]byte, msg.GetDataLen())

					//根据dataLen从io中读取字节流
					_, err := io.ReadFull(conn, msg.Data)
					if err != nil {
						fmt.Println("server unpack data err:", err)
						break
					}

					nodeMap[string(msg.Data)] = time.Now()
					fmt.Println(nodeMap)
				}
			}
		}

	}()

	go func() {
		localHost := "127.0.0.1:6431"
		time.Sleep(time.Second)
		conn, err := net.Dial("tcp4", "127.0.0.1:6430") // ESQ_SERVER
		if err != nil {
			fmt.Println("client start err ", err)
			return
		}
		defer conn.Close()

		for {
			time.Sleep(2 * time.Second)
			dp := enet.GetDataPack()
			msg := fmt.Sprintf("%s\t%s\n%s", topic_filereq, localHost, "涩图")
			data, _ := dp.Pack(enet.NewMsgPackage(0, []byte(msg)))
			//发封包message消息
			_, err = conn.Write(data)
			if err != nil {
				fmt.Println("write error err ", err)
				return
			}
		}
	}()

	s.Serve()
}
