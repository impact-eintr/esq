package main

import (
	"encoding/json"
	"fmt"
	"impact-eintr/esq/internal/gnode"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

var pushBase = "http://127.0.0.1:9504/push"
var popBase = "http://127.0.0.1:9504/pop?topic=%s&bindKey=%s"
var ackBase = "http://127.0.0.1:9504/ack?msgId=%s&topic=%s&bindKey=%s"

type Client struct{}

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

func (c *Client) Pop() {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(popBase, "heartbeat", "test"))
	if err != nil {
		log.Fatalln(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	m := make(map[string]interface{})
	json.Unmarshal(b, &m)

	msg := &gnode.RespMsgData{}
	b, _ = json.Marshal(m["data"])
	json.Unmarshal(b, &msg)

	c.ack(msg.Id)
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

func main() {
	cli := &Client{}
	cli.Push()
	cli.Pop()
}
