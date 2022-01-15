package mq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type LogFunc func(l LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

/*
 * =================================  一个落盘的channel =====================================
 *
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
	depth        int64 // 队列的长度

	sync.RWMutex

	// instantiation(实例化) time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // 一旦创建不可修改
	minMsgSize      int32
	maxMsgSize      int32
	syncEvery       int64         // 写入多少次要进行同步
	syncTimeout     time.Duration // 每次文件同步的时间间隔
	exitFlag        int32
	needSync        bool

	// keep track(追踪) of the position where we have read
	// 但是还没有通过 readChan 发送
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File      // 正在读取的文件
	writeFile *os.File      // 正在写入的文件
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
	logf LogFunc
}

// 将 []byte 写入队列
func (d *diskQueue) Put(data []byte) error {
	// TODO 为什么是只读保护
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	d.writeChan <- data
	return <-d.writeResponseChan
}

func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Close 会清除队列并保存元数据
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync() // 退出前先同步
}

// Delete 只会清除队列
func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// Empty 通过调用deleteAllFiles()
// 快速转发读取位置和删除中间文件来破坏性地清除队列中的任何待处理数据
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE@%s: emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan

}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE@%s: deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE@%s: closing", d.name)
	}

	close(d.exitChan)
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil

}

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
	syncEvery int64, syncTimeout time.Duration, logf LogFunc) Interface {
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
		d.logf(ERROR, "[DISKQUEUE]@%s failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()
	// 返回一个已经启动了相关机制的实例
	return &d
}

// 从元数据中恢复出一条可用的diskqueue
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName() // (d.name).diskqueue.meta.data
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// 格式：// (d.dataPath)/(d.name).diskqueue.meta.data
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.data"), d.name)
}

// 移除之前读过的文件
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1)

	// 检查是否需要删除已经读完的文件
	if oldReadFileNum != d.nextReadFileNum {
		// 每当我们要读取一个新文件的时候都要进行同步
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "[DISKQUEUE]@%s failed to Remove(%s) - %s", d.name, fn, err)
		}
	}
	// 检查
	d.checkTailCorruption(depth)
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	// 正常情况的话 返回
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// if readFileNum == writeFileNum && readPos == writePos => depth == 0
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"[DISKQUEUE]@%s negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"[DISKQUEUE]@%s positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// 强制设置 depth = 0
		atomic.StoreInt64(&d.depth, 0)
		d.needSync = true
	}

	// 不正常情况
	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"[DISKQUEUE]@%s readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting    0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(ERROR,
				"[DISKQUEUE]@%s readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

// 放弃之前所有不正常的文件 TODO
// 设置 readFileNum == writeFileNum && readPos == writePos 等同于清空队列(重开？)
func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	// 把不正常文件全部删除
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "[DISKQUEUE]@%s failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// 格式：(d.dataPath)/(d.name).diskqueue.(fileNum).data
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.data"), d.name, fileNum)
}

// readOne 为单个 []byte 执行低级文件系统读取，同时推进读取位置和滚动文件（如有必要）
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "[DISKQUEUE]@%s: readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
		d.reader = bufio.NewReader(d.readFile)
	}

	// 先读出来消息大小
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// 注意这里的 maxBytesPerFile 不是一个硬性规定(因为写的时候就没有严格限制大小)
	// 只是提示 reader 下次该去读下一个文件了
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}
		// 下次读下一个文件
		d.nextReadFileNum++
		d.nextReadPos = 0
	}
	return readBuf, nil
}

// writeOne 为单个 []byte 执行低级文件系统写入，同时推进写入位置和滚动文件（如有必要）
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		d.logf(INFO, "[DISKQUEUE]@%s: writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, io.SeekStart)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))
	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	d.writeBuf.Reset()
	// 写入长度 int32 4 bytes
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return nil
	}
	// 写入内容 dataLen bytes
	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// 文件只写一次
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64((&d.depth), 1)

	// 如果当前要写入的内容长度超过文件限制(非硬性要求 只是要求超过这个值后 下一条消息应该写在下一个文件)
	if d.writePos >= d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// 找人接盘的时候先同步
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "[DISKQUEUE]@%s failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	return err
}

func (d *diskQueue) sync() error {
	// 如果手里还有 写入文件 的指针 尝试同步
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	// 同步后 保存 元数据
	err := d.persistMetaData()
	if err != nil {
		return err
	}

	// 同步结束
	d.needSync = false
	return nil
}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "[DISKQUEUE]@%s failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tempFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// 先写到临时文件里
	f, err = os.OpenFile(tempFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()
	// 原子地改名(替换文件)
	return os.Rename(tempFileName, fileName)
}

// 出现错误的情况都是比较严重的 ReadAll() Seek() OpenFile() ...
func (d *diskQueue) handleReadError() {
	// 跳到下一个 read file 并重命名手上这个损坏的文件
	if d.readFileNum == d.writeFileNum {
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}
	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN, "[DISKQUEUE]@%s jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR, "[DISKQUEUE]@%s failed to rename bad diskqueue file %s to %s", d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// 重大状态变化，安排下一次迭代同步
	d.needSync = true
}

// 启动diskqueue的io循环机制
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64 // 操作文件计数器
	var r chan []byte

	// 同步定时器
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		if count == d.syncEvery {
			d.needSync = true
			d.logf(DEBUG, "[DISKQUEUE]@s: start to sync", d.name)
		}

		// 操作次数累积到应该同步的时候同步一下
		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "[DISKQUEUE]@s: failed to sync - %s", d.name, err)
			}
			count = 0
		}

		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "[DISKQUEUE]@%s reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError() // 处理读流程出现的错误
					continue
				}
			}
			r = d.readChan
		} else {
			// channel 规范规定跳过选择中的 nil 通道操作(读取或写入)
			// 我们仅在有数据要读取时将 r 设置为 d.readChan
			r = nil
		}

		select {
		case r <- dataRead:
			// moveForward() 在一个文件被移除后设置 needSync标志位
			d.moveForward()
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count == 0 {
				// 避免在不活跃的时候同步
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf(INFO, "[DISKQUEUE]@%s: closing ... ioloop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1 // 停止同步时钟后再退出
}
