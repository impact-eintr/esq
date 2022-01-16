package esq

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/impact-eintr/enet"
)

// esq 协议
// command topic srchost
// \n
// message

// 解析协议
func ParseCommand(data []byte) (command string) {
	first := strings.Split(string(data), "\n")[0]
	command = strings.Split(first, " ")[0]
	return
}

func ParseTopic(data []byte) (topic string) {
	first := strings.Split(string(data), "\n")[0]
	topic = strings.Split(first, " ")[1]
	return
}

func ParseSrcHost(data []byte) (srchost string) {
	first := strings.Split(string(data), "\n")[0]
	srchost = strings.Split(first, " ")[2]
	return
}

func ParseMsg(data []byte) (message string) {
	message = strings.Split(string(data), "\n")[1]
	return
}

// 封装协议
func packageProtocol(command, topic, src, msg string) []byte {
	return []byte(fmt.Sprintf("%s %s %s\n%s", command, topic, src, msg))
}

func PackageProtocol(msgId int, command, topic, src, msg string) []byte {
	dp := enet.GetDataPack()
	pkgMsg, _ := dp.Pack(enet.NewMsgPackage(0, packageProtocol(command, topic, src, msg)))
	return pkgMsg
}

// 单次读取
func ReadOnce(conn net.Conn) ([]byte, error) {
	dp := enet.GetDataPack()
	//先读出流中的head部分
	headData := make([]byte, dp.GetHeadLen())
	_, err := io.ReadFull(conn, headData) //ReadFull 会把msg填充满为止
	if err != nil {
		fmt.Println("read head error")
		return nil, err
	}
	//将headData字节流 拆包到msg中
	msgHead, err := dp.Unpack(headData)
	if err != nil {
		fmt.Println("server unpack err:", err)
		return nil, err
	}

	if msgHead.GetDataLen() <= 0 {
		return nil, errors.New("no any message")
	} else {
		//msg 是有data数据的，需要再次读取data数据
		msg := msgHead.(*enet.Message)
		msg.Data = make([]byte, msg.GetDataLen())

		//根据dataLen从io中读取字节流
		_, err = io.ReadFull(conn, msg.Data)
		if err != nil {
			fmt.Println("server unpack data err:", err)
			return nil, err
		}
		return msg.Data, nil
	}
}
