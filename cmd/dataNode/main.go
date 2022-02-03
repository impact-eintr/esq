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

func main() {

	go func() {
		for {
			time.Sleep(time.Second)
			conn, err := net.Dial("tcp4", "127.0.0.1:6430")
			if err != nil {
				fmt.Println("client start err ", err)
				continue
			}
			defer conn.Close()

			for {
				msg := esq.PackageProtocol(0, "PUB", topic_heartbeat, "1.1.1.1", "1.1.1.1:8080")
				//发封包message消息
				_, err = conn.Write(msg)
				if err != nil {
					fmt.Println("write error err ", err)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second)
			conn, err := net.Dial("tcp4", "127.0.0.1:6430")
			if err != nil {
				fmt.Println("client start err ", err)
				continue
			}
			defer conn.Close()

			for {
				msg := esq.PackageProtocol(0, "PUB", topic_heartbeat, "2.2.2.2", "2.2.2.2:8080")
				//发封包message消息
				_, err = conn.Write(msg)
				if err != nil {
					fmt.Println("write error err ", err)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	//go func() {
	//	time.Sleep(time.Second)
	//	conn, err := net.Dial("tcp4", "127.0.0.1:6430")
	//	if err != nil {
	//		fmt.Println("client start err ", err)
	//		return
	//	}
	//	defer conn.Close()

	//	//发封包message消息 只写一次
	//	msg := esq.PackageProtocol(0, "SUB", topic_filereq, "1.1.1.1", "有人要戒色?")
	//	_, err = conn.Write(msg)
	//	if err != nil {
	//		fmt.Println("write error err ", err)
	//		return
	//	}

	//	dp := enet.GetDataPack()
	//	// 不停地读
	//	for {
	//		//先读出流中的head部分
	//		headData := make([]byte, dp.GetHeadLen())
	//		_, err = io.ReadFull(conn, headData) //ReadFull 会把msg填充满为止
	//		if err != nil {
	//			fmt.Println("read head error")
	//			break
	//		}
	//		//将headData字节流 拆包到msg中
	//		msgHead, err := dp.Unpack(headData)
	//		if err != nil {
	//			fmt.Println("server unpack err:", err)
	//			return
	//		}

	//		if msgHead.GetDataLen() > 0 {
	//			//msg 是有data数据的，需要再次读取data数据
	//			msg := msgHead.(*enet.Message)
	//			msg.Data = make([]byte, msg.GetDataLen())

	//			//根据dataLen从io中读取字节流
	//			_, err := io.ReadFull(conn, msg.Data)
	//			if err != nil {
	//				fmt.Println("server unpack data err:", err)
	//				return
	//			}

	//			// 向ApiNode通信
	//			s := strings.Split(string(msg.GetData()), "\n")
	//			if s[1] != "遥感图" {
	//				continue
	//			}
	//			fmt.Println("addr:", s[0])
	//			c, err := net.Dial("tcp4", s[0])
	//			if err != nil {
	//				fmt.Println("client start err ", err)
	//				return
	//			}
	//			addr, _ := dp.Pack(enet.NewMsgPackage(20, []byte("1.1.1.1")))
	//			_, err = c.Write(addr)
	//			if err != nil {
	//				fmt.Println("write error err ", err)
	//				return
	//			}
	//			c.Close()

	//		}
	//	}
	//}()

	//go func() {
	//	time.Sleep(time.Second)
	//	conn, err := net.Dial("tcp4", "127.0.0.1:6430")
	//	if err != nil {
	//		fmt.Println("client start err ", err)
	//		return
	//	}
	//	defer conn.Close()

	//	//发封包message消息 只写一次
	//	msg := esq.PackageProtocol(0, "SUB", topic_filereq, "2.2.2.2", "有人要戒色?")
	//	_, err = conn.Write(msg)
	//	if err != nil {
	//		fmt.Println("write error err ", err)
	//		return
	//	}

	//	dp := enet.GetDataPack()
	//	// 不停地读
	//	for {
	//		//先读出流中的head部分
	//		headData := make([]byte, dp.GetHeadLen())
	//		_, err = io.ReadFull(conn, headData) //ReadFull 会把msg填充满为止
	//		if err != nil {
	//			fmt.Println("read head error")
	//			break
	//		}
	//		//将headData字节流 拆包到msg中
	//		msgHead, err := dp.Unpack(headData)
	//		if err != nil {
	//			fmt.Println("server unpack err:", err)
	//			return
	//		}

	//		if msgHead.GetDataLen() > 0 {
	//			//msg 是有data数据的，需要再次读取data数据
	//			msg := msgHead.(*enet.Message)
	//			msg.Data = make([]byte, msg.GetDataLen())

	//			//根据dataLen从io中读取字节流
	//			_, err := io.ReadFull(conn, msg.Data)
	//			if err != nil {
	//				fmt.Println("server unpack data err:", err)
	//				return
	//			}

	//			// 向ApiNode通信
	//			s := strings.Split(string(msg.GetData()), "\n")
	//			if s[1] != "涩图" {
	//				continue
	//			}
	//			fmt.Println("addr:", s[0])
	//			c, err := net.Dial("tcp4", s[0])
	//			if err != nil {
	//				fmt.Println("client start err ", err)
	//				return
	//			}
	//			addr, _ := dp.Pack(enet.NewMsgPackage(20, []byte("2.2.2.2")))
	//			_, err = c.Write(addr)
	//			if err != nil {
	//				fmt.Println("write error err ", err)
	//				return
	//			}
	//			c.Close()

	//		}
	//	}
	//}()
	select {}

}
