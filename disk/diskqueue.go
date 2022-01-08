package disk

import (
	"bufio"
	"bytes"
	"os"
	"sync"
	"time"
)

type LogLevel int

const (
	DEBIG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARN"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() chan []byte
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

/*
* 队列的特征是先入先出，也就是写入是从后面写入，读取是从前面读取
* 我们平时写的队列一般是放到内存里面，比如一个大的动态数组
* 这里如果队列中的数据很大，diskqueue 则是将这个动态数组拆成了好多个文件来存储队列中的数据
* 如果队列是放在内存数组中，那么队列只需要记录两个属性，一个头的位置，一个是尾的位置，
* 队列大小 depth = 头位置 - 尾位置
* 但是由于 diskqueue 是将数组保存在多个文件中
* 所以 diskqueue 就会有五个属性:
* 头所在的文件，头在文件中的位置，尾所在的文件，尾在文件中的位置，还有就是 depth 标识头和尾中间的数据数量
* 这五个数据作为 diskqueue 的元数据单独保存在一个文件里面。
* 所以 New 一个 diskqueue 的时候先要这几个元数据读取出来
 */
type diskQueue struct {
	// run-time state (这部分同样需要持久化落盘)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	sync.RWMutex

	// instantiation(实例化) time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // 一旦创建不可修改
	minMsgSize      int32
	maxMsgSize      int32
	syncEvery       int64         // 每次同步写入的文件数
	syncTimeout     time.Duration // 每次文件同步的时间间隔
	exitFlag        int32
	needSync        bool

	// keep track(追踪) of the position where we have read
	// 但是还没有通过 readChan 发送
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader // 读指针
	writeBuf  bytes.Buffer  // 写指针

	// exposed(暴露) via ReadChan()
	readChan chan []byte

	// 磁盘队列中内部使用的一些channel
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int

	// log function
	logf AppLogFunc
}

// TODO 使得 diskQueue成为一个Interface
func (d *diskQueue) Put([]byte) error
func (d *diskQueue) ReadChan() chan []byte
func (d *diskQueue) Close() error
func (d *diskQueue) Delete() error
func (d *diskQueue) Depth() int64
func (d *diskQueue) Empty() error

// 新的磁盘队列实例 New(...)
// Input:
// name             名字
// dataPath         文件保存目录
// maxBytesPerFile  每个文件最大尺寸
// minMsgSize       消息最小长度
// maxMsgSize       消息最大长度
// syncEvery        同步数
// syncTimeout      同步间隔
// logf             日志函数
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
	}

	// no need to lock here, nothing else could possibly be touching this instance(单例模式)
	err := d.retrieveMetaData() // 从元数据中恢复出一条可用的diskqueue
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()
	// 返回一个已经启动了相关机制的实例
	return &d
}

// 从元数据中恢复出一条可用的diskqueue
func (d *diskQueue) retrieveMetaData() error {
	// TODO
}

// 启动diskqueue的io循环机制
func (d *diskQueue) ioLoop() {
	// TODO
}
