package http_api

import (
	"net"
	"net/http"
	"time"
)

type Client struct {
	*http.Client
}

func NewClient(connTimeout time.Duration, reqTimeout time.Duration) *Client {
	transport := NewDeadlineTransport(connTimeout, reqTimeout)
	return &Client{
		&http.Client{
			Transport: transport,
			Timeout:   reqTimeout,
		},
	}
}

func NewDeadlineTransport(connTimeout time.Duration,
	reqTimeout time.Duration) *http.Transport {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   connTimeout,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ResponseHeaderTimeout: reqTimeout,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
	}
	return transport

}
