package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/impact-eintr/esq/gnode"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	RESP_MESSAGE = 101
	RESP_ERROR   = 102
	RESP_RESULT  = 103
)

var (
	ErrTopicEmpty   = errors.New("topic is empty")
	ErrTopicChannel = errors.New("channel is empty")
)

var (
	cli1 = "clientNo.1"
	cli2 = "clientNo.2"
)

var (
	pushBase    = "http://%s/push"
	declereBase = "http://%s/declareQueue?topic=%s&bindKey=%s"
	configBase  = "http://%s/config?topic=%s&isAutoAck=%d&mode=%d&msgTTR=%d&msgRetry=%d"
	popBase     = "http://%s/pop?topic=%s&bindKey=%s"
	ackBase     = "http://%s/ack?msgId=%s&topic=%s&bindKey=%s"
)

type MsgPkg struct {
	Body     string `json:"body"`
	Topic    string `json:"topic"`
	Delay    int    `json:"delay"`
	RouteKey string `json:"route_key"`
}

type MMsgPkg struct {
	Body  string
	Delay int
}

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

type Client struct {
	//conn     net.Conn
	addr   string
	weight int
	WaitGroupWrapper
}

// 初始化客户端,建立和注册中心节点连接
func NewClient(addr string, weight int) *Client {
	if len(addr) == 0 {
		log.Fatalln("address is empty")
	}

	resp, err := http.Get("http://" + addr + "/ping")
	if err != nil {
		log.Fatalln(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil || string(b) != "OK" {
		log.Println(string(b), err)
		return nil
	}

	return &Client{
		addr:   addr,
		weight: weight,
	}
}

func (c *Client) Exit() {}

func (c *Client) Push(msg, topic, routeKey string, delay int) {
	cli := &http.Client{}
	data := fmt.Sprintf(`data={"body":"%s","topic":"%s","delay":%d,"route_key":"%s"}`,
		msg, topic, delay, routeKey)
	resp, err := cli.Post(fmt.Sprintf(pushBase, c.addr), "application/x-www-form-urlencoded",
		strings.NewReader(data))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
}

func (c *Client) Pop(topic, bindKey string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(popBase, c.addr, topic, bindKey))
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

	if m["data"] != nil {
		msg := &gnode.RespMsgData{}
		b, err = json.Marshal(m["data"])
		err = json.Unmarshal(b, &msg)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(bindKey, "消费了", msg.Body)
		c.ack(topic, bindKey, msg.Id)
	}
}

func (c *Client) declare(topic, bindKey string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(declereBase, c.addr, topic, bindKey))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
}

func (c *Client) config(topic string, isAutoAck, mode, msgTTR, msgRetry int) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(configBase, c.addr, topic, isAutoAck, mode, msgTTR, msgRetry))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
}

func (c *Client) ack(topic, bindKey, id string) {
	cli := &http.Client{}
	resp, err := cli.Get(fmt.Sprintf(ackBase, c.addr, id, topic, bindKey))
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
}

var clients []*Client

func InitClients(endpoints string) ([]*Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("endpoints is empty.")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("can't new etcd client.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err := cli.Get(ctx, "/esq/node", clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, err
	}

	var clients []*Client
	node := make(map[string]string)
	for _, ev := range resp.Kvs {
		fmt.Printf("%s => %s\n", ev.Key, ev.Value)
		if err := json.Unmarshal(ev.Value, &node); err != nil {
			return nil, err
		}

		//tcpAddr := node["tcp_addr"]
		httpAddr := node["http_addr"]
		weight, _ := strconv.Atoi(node["weight"])
		c := NewClient(httpAddr, weight)
		clients = append(clients, c)
	}

	return clients, nil
}

// 权重模式
func GetClientByWeightMode(endpoints string) *Client {
	if len(clients) == 0 {
		var err error
		clients, err = InitClients(endpoints)
		if err != nil {
			log.Fatalln(err)
		}
	}

	total := 0
	for _, c := range clients {
		total += c.weight
	}

	w := 0
	rand.Seed(time.Now().UnixNano())
	randValue := rand.Intn(total) + 1
	for _, c := range clients {
		prev := w
		w = w + c.weight
		if randValue > prev && randValue <= w {
			return c
		}
	}

	return nil
}

// 根据最大权重选择节点
func GetClientByMaxWeightMode(endpoints string) *Client {
	if len(clients) == 0 {
		var err error
		clients, err = InitClients(endpoints)
		if err != nil {
			log.Fatalln(err)
		}
	}

	max := 0
	for _, c := range clients {
		if max < c.weight {
			max = c.weight
		}
	}

	for _, c := range clients {
		if c.weight == max {
			return c
		}
	}

	return nil
}

// 随机模式
func GetClientByRandomMode(endpoints string) *Client {
	if len(clients) == 0 {
		var err error
		clients, err = InitClients(endpoints)
		if err != nil {
			log.Fatalln(err)
		}
	}

	rand.Seed(time.Now().UnixNano())
	k := rand.Intn(len(clients))
	return clients[k]
}

// 平均模式
func GetClientByAvgMode(endpoints string) *Client {
	if len(clients) == 0 {
		var err error
		clients, err = InitClients(endpoints)
		if err != nil {
			log.Fatalln(err)
		}
	}

	c := clients[0]
	if len(clients) > 1 {
		// 已处理过的消息客户端重新放在最后
		clients = append(clients[1:], c)
	}

	return c
}

func main() {
	var cli = new(Client)
	var etcdEndPoint string
	flag.StringVar(&etcdEndPoint, "raftd_endpoint", "127.0.0.1:2379", "raftd endpoint")
	flag.Parse()

	switch os.Args[1] {
	case "1":
		cli = GetClientByWeightMode(etcdEndPoint)
	case "2":
		cli = GetClientByMaxWeightMode(etcdEndPoint)
	case "3":
		cli = GetClientByRandomMode(etcdEndPoint)
	case "4":
		cli = GetClientByAvgMode(etcdEndPoint)
	default:
		log.Fatalf("invalid type %s, should be 1：权重 2：最大权重 3：随机 4：平均", os.Args[1])
	}

	// curl -s "http://cli.addr/config?topic=heartbeat&isAutoAck=1&mode=2&msgTTR=30&msgRetry=5"
	cli.config("heartbeat", 0, 2, 30, 5)
	cli.declare("heartbeat", cli1)
	cli.declare("heartbeat", cli2)

	var MAX = 5

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
		}
	})

	cli.Wait()
}
