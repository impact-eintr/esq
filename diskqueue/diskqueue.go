package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	"github.com/impact-eintr/esq/internal/lg"
)

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

type diskQueue struct {
	// 运行时状态
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	sync.RWMutex

	// 实例化 time metadata
	name                string
	dataPath            string
	maxBytesPerFile     int64
	maxBytesPerFileRead int64
	minMsgSize          int32
	maxMsgSize          int32
	syncEvery           int64
	syncTimeout         time.Duration
	exitFlag            int32
	needSync            bool

	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// 通过 ReadChan() 暴露
	readCh chan []byte

	// internal channels
	depthCh         chan int64
	writeCh         chan []byte
	writeResponseCh chan error
	emptyCh         chan int
	emptyResponseCh chan error
	exitCh          chan int
	exitSyncCh      chan int

	logf lg.LogFunc
}

func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf lg.LogFunc) Interface {
	d := diskQueue{
		name:            name,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		minMsgSize:      minMsgSize,
		maxMsgSize:      maxMsgSize,
		readCh:          make(chan []byte),
		depthCh:         make(chan int64),
		writeCh:         make(chan []byte),
		writeResponseCh: make(chan error),
		emptyCh:         make(chan int),
		emptyResponseCh: make(chan error),
	}

	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(lg.ERROR, "DISKQUEU(%s) faild to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()
	return &d

}

func (d *diskQueue) Depth() int64 {

}

func (d *diskQueue) Put(data []byte) error {

}

func (d *diskQueue) Close() error {

}

func (d *diskQueue) Delete() error {

}

func (d *diskQueue) Empty() error {

}

func (d *diskQueue) ReadChan() <-chan []byte {

}

func (d *diskQueue) retrieveMetaData() error {

}

func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		if count == d.syncEvery {
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(lg.ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}

		// []file 中 i(readfile) < j(writefile) || i == j 读指针在写指针之前
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(lg.ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
				r = d.readCh // 只有在有数据要读时才将r设置为d.readChan
			} else {
				r = nil // channel 规范规定跳过select中的nil通道操作（读或写），
			}
		}

		select {
		case r <- dataRead:
			count++
			log.Println("case r <- dataRead : count = ", count)
			d.moveForward()
		case d.depthCh <- d.depth:
		case <-d.emptyCh:
			d.emptyResponseCh <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeCh:
			count++
			d.writeResponseCh <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count == 0 {
				continue
			}
			d.needSync = true
		case <-d.exitCh:
			goto exit
		}
	}
exit:
	d.logf(lg.INFO, "DISKQUEUE(%s): closeing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncCh <- 1

}

func (d *diskQueue) sync() error {
	// 把正在写的文件同步到磁盘
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false // 同步结束
	return nil
}

func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmpfile
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		d.depth,
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}
		d.logf(lg.INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		// 如果保存了读位置
		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.maxBytesPerFileRead = d.maxBytesPerFile
		if d.readFileNum < d.writeFileNum {
			stat, err := d.readFile.Stat()
			if err == nil {
				d.maxBytesPerFileRead = stat.Size() // 确定最大读取值
			}
		}
		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize) // 获取消息长度
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf) // 读取数据
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// 我们仅在读取“完整”文件时才考虑旋转，
	// 并且由于我们无法知道旋转的大小，
	// 因此依赖 maxBytesPerFileRead 而不是 maxBytesPerFile
	if d.readFileNum < d.writeFileNum && d.nextReadPos >= d.maxBytesPerFileRead {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}
	return readBuf, nil

}

func (d *diskQueue) handleReadError() {
	// 跳过下一个 read file 并且重命名当前损坏文件
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

	d.logf(lg.WARN, "DISKQUEUE(%s) jump to next file and saving bad file %s as %s",
		d.name, badFn, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(lg.ERROR, "DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	d.needSync = true

}

func (d *diskQueue) moveForward() {
	oldReadNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	d.depth--

	// 检查是否需要清除旧文件
	if oldReadNum != d.nextReadFileNum {
		d.needSync = true

		dn := d.fileName(oldReadNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(lg.ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, dn, err)
		}
	}
	d.checkTailCorruption(d.depth)
}

func (d *diskQueue) writeOne(data []byte) error {

}
