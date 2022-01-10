package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/impact-eintr/enet"
)

var (
	topic_heartbeat = "心跳"
	topic_filereq   = "文件定位请求"
)

func main() {

	go func() {
		time.Sleep(time.Second)
		conn, err := net.Dial("tcp4", "127.0.0.1:6430")
		if err != nil {
			fmt.Println("client start err ", err)
			return
		}
		defer conn.Close()

		for {
			dp := enet.GetDataPack()
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
		dp := enet.GetDataPack()
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
				if s[1] != "遥感图" {
					continue
				}
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

	go func() {
		time.Sleep(time.Second)
		conn, err := net.Dial("tcp4", "127.0.0.1:6430")
		if err != nil {
			fmt.Println("client start err ", err)
			return
		}
		defer conn.Close()

		for {
			dp := enet.GetDataPack()
			msg, _ := dp.Pack(enet.NewMsgPackage(0, []byte(topic_heartbeat+"\t"+"2.2.2.2")))
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
		dp := enet.GetDataPack()
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
				if s[1] != "涩图" {
					continue
				}
				fmt.Println("addr:", s[0])
				c, err := net.Dial("tcp4", s[0])
				if err != nil {
					fmt.Println("client start err ", err)
					return
				}
				addr, _ := dp.Pack(enet.NewMsgPackage(20, []byte("2.2.2.2")))
				_, err = c.Write(addr)
				if err != nil {
					fmt.Println("write error err ", err)
					return
				}
				c.Close()

			}
		}
	}()
	select {}

}
