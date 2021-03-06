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

	"github.com/impact-eintr/esq/gnode"
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
var declereBase = "http://127.0.0.1:9504/declareQueue?topic=%s&bindKey=%s"
var configBase = "http://127.0.0.1:9504/config?topic=%s&isAutoAck=%d&mode=%d&msgTTR=%d&msgRetry=%d"
var popBase = "http://127.0.0.1:9504/pop?topic=%s&bindKey=%s"
var ackBase = "http://127.0.0.1:9504/ack?msgId=%s&topic=%s&bindKey=%s"

type Client struct {
	WaitGroupWrapper
}

func (c *Client) Push(msg, topic, routeKey string, delay int) {
	cli := &http.Client{}
	data := fmt.Sprintf(`data={"body":"%s","topic":"%s","delay":%d,"route_key":"%s"}`,
		msg, topic, delay, routeKey)
	resp, err := cli.Post(pushBase, "application/x-www-form-urlencoded", strings.NewReader(data))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
}

func (c *Client) Pop(topic, bindKey string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(popBase, topic, bindKey))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	m := make(map[string]interface{})
	json.Unmarshal(b, &m)

	msg := &gnode.RespMsgData{}
	b, err = json.Marshal(m["data"])
	err = json.Unmarshal(b, &msg)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(bindKey, "消费了", msg.Body)
	c.ack(topic, bindKey, msg.Id)
}

func (c *Client) declare(topic, bindKey string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(declereBase, topic, bindKey))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
}

func (c *Client) config(topic string, isAutoAck, mode, msgTTR, msgRetry int) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(configBase, topic, isAutoAck, mode, msgTTR, msgRetry))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
}

func (c *Client) ack(topic, bindKey, id string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(ackBase, id, topic, bindKey))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
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

	// curl -s "http://127.0.0.1:9504/config?topic=heartbeat&isAutoAck=1&mode=2&msgTTR=30&msgRetry=5"
	cli.config("heartbeat", 0, 2, 30, 5)
	cli.declare("heartbeat", cli1)
	cli.declare("heartbeat", cli2)
	cli.declare("heartbeat", cli3)
	cli.declare("heartbeat", cli4)
	cli.declare("heartbeat", cli5)
	cli.declare("heartbeat", cli6)

	var MAX = 5000

	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Push("Ping...", "heartbeat", "client*", 0)
			time.Sleep(1 * time.Millisecond)
		}
	})

	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop("heartbeat", cli1)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop("heartbeat", cli2)
			time.Sleep(5 * time.Millisecond)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop("heartbeat", cli3)
			time.Sleep(2 * time.Millisecond)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop("heartbeat", cli4)
			time.Sleep(10 * time.Millisecond)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop("heartbeat", cli5)
		}
	})
	cli.Wrap(func() {
		for i := 0; i < MAX; i++ {
			cli.Pop("heartbeat", cli6)
		}
	})

	cli.Wait()

}
