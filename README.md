# esq

## 这是个什么

轻量级消息队列，是对 <https://github.com/wuzhc/gmq> 的魔改，看看正经的轻量级消息队列应该如何设计，大有裨益。

## 使用方法

需要安装 etcd 并运行
``` sh
etcd
```

启动 esq 的单个节点
``` sh
cd cmd/gnode

go build

./gnode -http_addr=":9504" -tcp_addr=":9503" -etcd_endpoints="127.0.0.1:2379" -node_id=1 -node_weight=1
```

### 简单的客户端测试 —— 发送与接受心跳

``` sh
cd cmd/gcli

curl -s "http://127.0.0.1:9504/declareQueue?topic=heartbeat&bindKey=test"

curl -s "http://127.0.0.1:9504/config?topic=heartbeat&isAutoAck=1&isMultiple=1&mode=1&msgTTR=5&msgRetry=5

go run ./main.go
```

### 客户端源码

``` go
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

var pushBase = "http://127.0.0.1:9504/push"
var popBase = "http://127.0.0.1:9504/pop?topic=%s&bindKey=%s&clientID=%s"
var ackBase = "http://127.0.0.1:9504/ack?msgId=%s&topic=%s&bindKey=%s"
var multipleBase = "http://127.0.0.1:9504/multiple?topic=%s&bindKey=%s&clientID=%s"

type Client struct {
	WaitGroupWrapper
}

func (c *Client) Push() {
	cli := &http.Client{}
	data := fmt.Sprintf(`data={"body":"%s","topic":"%s","delay":%d,"route_key":"%s"}`,
		"ping...", "heartbeat", 0, "test")
	resp, err := cli.Post(pushBase, "application/x-www-form-urlencoded", strings.NewReader(data))
	if err != nil {
		log.Fatalln(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(b))
}

func (c *Client) Pop(clientID string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(popBase, "heartbeat", "test", clientID))
	if err != nil {
		log.Fatalln(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	m := make(map[string]interface{})
	json.Unmarshal(b, &m)
	log.Println(m)

	//msg := &gnode.RespMsgData{}
	//b, _ = json.Marshal(m["data"])
	//json.Unmarshal(b, &msg)

	//log.Printf("%#v", *msg)
	//c.ack(msg.Id)
}

func (c *Client) ack(id string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(ackBase, id, "heartbeat", "test"))
	if err != nil {
		log.Fatalln(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(b))
}

// curl "http://127.0.0.1:9504/multiple?topic=xxx&bindKey=xxx&clientID=xxx"
func (c *Client) multiple(id string) {
	cli := &http.Client{}
	_, err := cli.Get(fmt.Sprintf(multipleBase, "heartbeat", "test", id))
	if err != nil {
		log.Fatalln(err)
	}
}

var (
	cli1 = "clientNo.1"
	cli2 = "clientNo.2"
	cli3 = "clientNo.3"
	cli4 = "clientNo.4"
	cli5 = "clientNo.5"
	cli6 = "clientNo.6"
)

func main() {
	cli := &Client{}
	cli.multiple(cli1)
	cli.multiple(cli2)
	cli.multiple(cli3)
	cli.multiple(cli4)
	cli.multiple(cli5)
	cli.multiple(cli6)

	var MAX = 5000

	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Push()
			time.Sleep(1 * time.Millisecond)
		}
	})

	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop(cli1)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop(cli2)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop(cli3)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop(cli4)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop(cli5)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop(cli6)
		}
	})

	cli.Wait()

}

```

## 说点什么

感谢伟大的开源运动，让我能看到这么多优秀的前辈留下的代码，给我这个菜鸡开个大眼。
