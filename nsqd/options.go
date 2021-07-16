package esqd

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/impact-eintr/esq/internal/lg"
)

type Options struct {
	ID int64

	// 设置日志
	LogLevel  lg.LogLevel
	LogPrefix string
	Logger    Logger

	// 设置服务
	TCPAddress            string
	HTTPAddress           string
	BroadcastAddress      string
	NSQLookupdTCPAddress  []string
	HTTPClientConnTimeout time.Duration
	HTTPClientReqTimeout  time.Duration

	// diskqueue 配置
	DataPath     string
	MemQueueSize int64
	MaxFileSize  int64
	SyncEvery    int64
	SyncTimeout  time.Duration

	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int64
	QueueScanWorkerPoolMax   int64
	QueueScanDirtyPercent    float64

	// 消息与命令配置

	// 状态集
	StatsAddress       string
	StatsPrefix        string
	StatsInterval      time.Duration
	StatsMemStats      bool
	StatsUDPPacketSize int
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID: defaultID,

		TCPAddress:       "0.0.0.0:6430",
		HTTPAddress:      "0.0.0.0:6431",
		BroadcastAddress: hostname,

		MemQueueSize: 10000,
		MaxFileSize:  100 * 1024 * 1024,
		SyncEvery:    2500,
		SyncTimeout:  2 * time.Second,
	}
}
