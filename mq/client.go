package mq

import (
	"time"
)

type Client struct {
	bro *BrokerImpl
}

func NewClient(dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration) *Client {
	return &Client{
		bro: NewBroker(dataPath, maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery, syncTimeout),
	}
}

func (c *Client) Publish(topic string, msg []byte) error {
	return c.bro.publish(topic, msg)
}

func (c *Client) Subscribe(topic string, src string) (Interface, error) {
	return c.bro.subscribe(topic, src)
}

// 取消订阅，传入订阅的主题和对应的通道
func (c *Client) HandleSubscribeError(topic string, sub Interface) error {
	return c.bro.unsubscribe(topic, sub, false)
}

func (c *Client) Unsubscribe(topic string, subname string) error {
	var sub Interface

	c.bro.RLock()
	for idx := range c.bro.topics[topic] {
		if c.bro.topics[topic][idx].Name() == subname {
			sub = c.bro.topics[topic][idx]
			break
		}
	}
	c.bro.RUnlock()

	return c.bro.unsubscribe(topic, sub, true)
}

func (c *Client) Close() {
	c.bro.close()
}

func (c *Client) GetPayLoad(sub Interface) []byte {
	for val := range sub.ReadChan() {
		if val != nil {
			return val
		}
	}
	return nil
}
