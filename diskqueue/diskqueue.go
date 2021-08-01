package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

/*队列的特征是先入先出，也就是写入是从后面写入，读取是从前面读取
我们平时写的队列一般是放到内存里面，比如一个大的动态数组
这里如果队列中的数据很大，diskqueue 则是将这个动态数组拆成了好多个文件来存储队列中的数据
如果队列是放在内存数组中，那么队列只需要记录两个属性，一个头的位置，一个是尾的位置，
队列大小 depth = 头位置 - 尾位置
但是由于 diskqueue 是将数组保存在多个文件中
所以 diskqueue 就会有五个属性： 头所在的文件，头在文件中的位置，尾所在的文件，尾在文件中的位置，还有就是 depth 标识头和尾中间的数据数量
这五个数据作为 diskqueue 的元数据单独保存在一个文件里面。
所以 New 一个 diskqueue 的时候先要这几个元数据读取出来
*/
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

	logf LogFunc
}

func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf LogFunc) Interface {
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
		exitCh:          make(chan int),
		exitSyncCh:      make(chan int),
		syncEvery:       syncEvery,
		syncTimeout:     syncTimeout,
		logf:            logf,
	}

	// 读取队列数据 这里不需要锁 该实例现阶段不对外开放
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEU(%s) faild to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()
	return &d

}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1
	if deleted {
		d.logf(INFO, "DISKQUEUE[%s]: deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE[%s]: closing", d.name)
	}

	close(d.exitCh)

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

func (d *diskQueue) Depth() int64 {
	depth, ok := <-d.depthCh
	if !ok {
		depth = d.depth
	}
	return depth
}

func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	d.writeCh <- data // 写数据
	return <-d.writeResponseCh
}

func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE[%s]: emptying", d.name)
	d.emptyCh <- 1
	return <-d.emptyResponseCh
}

func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readCh
}

// 恢复元数据
// 包括 d.depth d.readFileNum d.readPos d.writeFileNum d.writePos
// d.nextReadFileNum d.nextReadPos
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// 队列写入和读取位置中间有多少条数据也就是队列的大小
	var depth int64
	// 读取队列核心数据
	// 当前读取文件是哪个，读取位置是哪里
	// 当前写入的文件是哪个，写入文件位置
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth, &d.readFileNum, &d.readPos, &d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.depth = depth
	// 下一个读取文件
	d.nextReadFileNum = d.readFileNum
	// 下一个读取位置
	d.nextReadPos = d.readPos
	// 如果在 nsqd 上次关闭时元数据没有同步，
	// 那么实际文件大小实际上可能比 writePos 大，
	// 在这种情况下，最安全的做法是跳到下一个文件进行写入，
	// 并让读取器从diskqueue中的消息中抢救出超出元数据可能也过时的readPos的内容
	fileName = d.fileName(d.writeFileNum)
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	if d.writePos < fileSize {
		d.logf(WARN, "DISKQUEUE[%s] %s metadata writePos %d size of %d, skiping to new file",
			d.name, fileName, d.writePos, fileSize)
		d.writeFileNum += 1
		d.writePos = 0
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	return nil
}

// 这个函数是一个“守护”协程，
// 暴露的 d.writeChan 和 d.readChan
// 如果外部有往 writeChan 里写数据在这里处理
// 同时，这里的消息也会通过 d.readChan 将消息不断的从队列中往外推
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	// 开启一个timer syncTimeout在系统配置里 2s
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// SyncEvery 是系统配置 默认是2500
		if count == d.syncEvery {
			d.needSync = true
		}
		// neddSync == true 缓存中的数据保存到磁盘
		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE[%s] failed to sync - %s", d.name, err)
			}
			// 同步到磁盘成功 将count 重置为0
			count = 0
		}

		// write> ==== >read
		// []file 中 i(readfile) < j(writefile) || i == j 读指针在写指针之前
		// 如果队列头和尾之间还有数据 则从头部读取数据
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			// 这里读取的数据放到readCh这个channel中
			// 而且只有在有数据要读时才将r设置为d.readChan
			r = d.readCh
		} else {
			r = nil // channel 规范规定跳过select中的nil通道操作（读或写），
		}

		select {
		case r <- dataRead: // dataRead 是 []byte
			count++
			d.moveForward()
		case d.depthCh <- d.depth:
		case <-d.emptyCh:
			d.emptyResponseCh <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeCh:
			// 如果d.writeChan有写入数据 则将消息数据写入到队列
			count++
			d.writeResponseCh <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			// 这里相当于2s同步一次到磁盘
			if count == 0 {
				continue
			}
			d.needSync = true
		case <-d.exitCh:
			goto exit
		}
	}
exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncCh <- 1

}

func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		// 将缓冲区的数据从内存中拷贝刷新到硬盘中保存
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}
	// 保存元数据
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

	// 当前读取文件是否打开 没有则打开当前文件
	if d.readFile == nil {
		// 打开读取的文件（当前队列头所在的文件）
		// 如果文件大小达到上限 readFileNum++
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}
		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		// 当前队列头在当前读取文件的位置
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
				log.Println(stat.Size())
				d.maxBytesPerFileRead = stat.Size() // 确定最大读取值为当前不完整文件的大小
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
	// 判断消息大小是否合法
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

	// 将下一个要读取的位置往后移
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// 我们仅在读取“完整”文件时才循环读，
	// 并且由于我们无法知道循环读的次数，
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

	d.logf(WARN, "DISKQUEUE(%s) jump to next file and saving bad file %s as %s",
		d.name, badFn, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	d.needSync = true

}

// 会删除已经没用的file 当前文件的数据已经被全部读取了 不在队列头尾之间了
func (d *diskQueue) moveForward() {
	oldReadNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	d.depth -= 1

	// 检查是否需要清除旧文件
	if oldReadNum != d.nextReadFileNum {
		d.needSync = true

		dn := d.fileName(oldReadNum)
		err := os.Remove(dn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, dn, err)
		}
	}
	d.checkTailCorruption(d.depth)
}

// 检测尾部数据损毁
func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR, "DISKQUEUE[%s] metadata 损坏 depth(%d) < 0, 重置为 0", d.name, d.depth)
		} else if depth > 0 {
			d.logf(ERROR, "DISKQUEUE[%s] metadata 损坏 depth(%d) > 0, 重置为 0", d.name, d.depth)
		}
		d.depth = 0
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption,skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}

}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		return innerErr
	}
	return err

}

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
	log.Printf("readFileNum %d writeFileNum %d\n", d.readFileNum, d.writeFileNum)
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE[%s] failed to remove metadata file - %s", d.name, innerErr)
			err = innerErr
		}
	}
	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	d.depth = 0

	return err

}

func (d *diskQueue) writeOne(data []byte) error {
	var err error

	// 当前写入文件是否打开 没有打开则打开当前写入文件
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil
		}

		d.logf(INFO, "DISKQUEUE[%s]: writeOne() opened %s", d.name, curFileName)

		// 如果当前写入位置大于0 则将文件位置移动到写入位置点

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}
	dataLen := int32(len(data))

	//判断消息大小是否合法
	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	//将缓冲区清空
	d.writeBuf.Reset()
	//将消息大小写入缓冲区
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	//将消息写入缓冲区
	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	//将缓冲区关联到文件
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	//计算总大小
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	//队列消息数量+1
	d.depth += 1

	//如果写入位置大于了文件最大大小
	if d.writePos >= d.maxBytesPerFile {
		if d.readFileNum == d.writeFileNum {
			d.maxBytesPerFileRead = d.writePos
		}

		//将当前写入文件+1
		d.writeFileNum++
		//当前写入位置重置为0
		d.writePos = 0

		// 写完一个完整文件后 将缓存数据写入到磁盘
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE[%s] failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	return err

}
