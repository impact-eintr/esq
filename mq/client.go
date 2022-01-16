package mq

import "time"

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

func (c *Client) Unsubscribe(topic string, sub Interface) error {
	return c.bro.unsubscribe(topic, sub)
}

func (c *Client) Close() {
	c.bro.close()
}

func (c *Client) GetPayLoad(sub Interface) interface{} {
	for val := range sub.ReadChan() {
		if val != nil {
			return val
		}
	}
	return nil
}
