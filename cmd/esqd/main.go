package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/impact-eintr/enet"
	"github.com/impact-eintr/enet/iface"
	"github.com/impact-eintr/esq/mq"
)

var (
	topic_heartbeat = "心跳"
	topic_filereq   = "文件定位请求"
)

type PubRouter struct {
	enet.BaseRouter
}

func (this *PubRouter) Handle(req iface.IRequest) {
	msgs := strings.Split(string(req.GetData()), "\t")
	Pub(msgs[0], msgs[1], m, nil)
}

type SubRouter struct {
	enet.BaseRouter
}

func (this *SubRouter) Handle(req iface.IRequest) {
	msgs := strings.Split(string(req.GetData()), "\t")
	go Sub(msgs[0], m, func(v interface{}, ch chan bool) {
		err := req.GetConnection().SendTcpMsg(2020, []byte(v.(string)))
		if err != nil {
			ch <- true
			fmt.Println("退出原因：", err)
		}
	})
}

var m = mq.NewClient()

func main() {
	m.SetConditions(10)
	s := enet.NewServer("tcp4")
	// 添加路由
	s.AddRouter(0, &PubRouter{})  // Publish
	s.AddRouter(10, &SubRouter{}) // Subscribe

	go func() {
		time.Sleep(time.Second)
		conn, err := net.Dial("tcp4", "127.0.0.1:6430")
		if err != nil {
			fmt.Println("client start err ", err)
			return
		}
		defer conn.Close()

		for {
			dp := enet.NewDataPack()
			msg, _ := dp.Pack(enet.NewMsgPackage(0, []byte(topic_heartbeat+"\t"+"1.1.1.1")))
			//发封包message消息
			_, err = conn.Write(msg)
			if err != nil {
				fmt.Println("write error err ", err)
				return
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	go func() {
		time.Sleep(time.Second)
		conn, err := net.Dial("tcp4", "127.0.0.1:6430")
		if err != nil {
			fmt.Println("client start err ", err)
			return
		}
		defer conn.Close()

		//发封包message消息 只写一次
		dp := enet.NewDataPack()
		msg, _ := dp.Pack(enet.NewMsgPackage(10, []byte(topic_filereq+"\t"+"谁想要要色图")))
		_, err = conn.Write(msg)
		if err != nil {
			fmt.Println("write error err ", err)
			return
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
				return
			}

			if msgHead.GetDataLen() > 0 {
				//msg 是有data数据的，需要再次读取data数据
				msg := msgHead.(*enet.Message)
				msg.Data = make([]byte, msg.GetDataLen())

				//根据dataLen从io中读取字节流
				_, err := io.ReadFull(conn, msg.Data)
				if err != nil {
					fmt.Println("server unpack data err:", err)
					return
				}

				// 向ApiNode通信
				s := strings.Split(string(msg.GetData()), "\n")
				fmt.Println("addr:", s[0])
				c, err := net.Dial("tcp4", s[0])
				if err != nil {
					fmt.Println("client start err ", err)
					return
				}
				addr, _ := dp.Pack(enet.NewMsgPackage(20, []byte("1.1.1.1")))
				_, err = c.Write(addr)
				if err != nil {
					fmt.Println("write error err ", err)
					return
				}
				c.Close()

			}
		}
	}()

	s.Serve()
}

type Pubfunc func(interface{})

func Pub(topic string, pubmsg string, mc *mq.Client, cb Pubfunc) {
	// 发布消息
	err := mc.Publish(topic, pubmsg)
	if err != nil {
		log.Println("publish message failed")
	}
	// 调用回调函数
	if cb != nil {
		cb(nil)
	}
}

type Subfunc func(interface{}, chan bool)

// 订阅者一旦订阅就需要一直监听 因此需要提供一个退出的情况
func Sub(topic string, mc *mq.Client, cb Subfunc) {
	ch, err := mc.Subscribe(topic)
	if err != nil {
		log.Println("subscribe failed")
		return
	}

	exitCh := make(chan bool, 1)
	for {
		select {
		case <-exitCh:
			fmt.Println("Sub 退出")
			return
		default:
			// 先看看消息是个啥
			val := mc.GetPayLoad(ch)
			fmt.Printf("get message is %s\n", val)
			// 有回调函数就调用
			if cb != nil {
				cb(val, exitCh)
			}
		}
	}
}
