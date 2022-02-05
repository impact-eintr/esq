// 初始化:
//  - wfid为0,woffset为0
//  - rfid为0,roffset为0
//  - 初始化一个totalSize大小的文件,内容为0,totalSzie大小为pageSize的整数倍

// 写(生产消息):
//  - wfid为写文件编号,woffset为当前写偏移量,两个字段表示当前写到哪个文件哪个位置
//  - 维护一个wfid和offset的map表
//  - wfid为0,初始化一个totalSize大小的文件,内容为0,totalSzie大小为pageSize的整数倍,执行映射,wfid加1
//  - 根据woffset,写入内容,更新woffset

// 读(消费消息):
//  - rfid为读文件编号,roffset为当前读偏移量,两个字段表示当前读到哪个文件哪个位置
//  - rfid为0,rfid加1,查看文件是否存在,存在则映射
//  - 根据roffset和woffset读取内容,更新roffset
//  - 读取完毕,删除写步骤的map表记录

// 扫(确认消息):
//  - sfid为扫文件编号,soffset为当前扫偏移量,两个字段表示当前扫描到哪个文件哪个位置
//  - sfid为0,sfid加1,查看文件是否存在,存在则映射
//  - 根据soffset和roffset读取内容,更新soffset
//  - 扫描完毕,删除数据文件,删除读步骤的map表记录
package gnode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"impact-eintr/esq/pkg/logs"
	"impact-eintr/esq/pkg/utils"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// header: flag(1 byte)+status(2 byte)+msg_len(4 byte)+msg(? byte)
const MSG_FIX_LENGTH = 7
const GROW_SIZE = 10 * 1024 * 1024
const REWRITE_SIZE = 100 * 1024 * 1024

// 局部错误变量
var (
	ErrQueueClosed = errors.New("queue has been cloesd")
)

// queue 队列,消息存储地方
type queue struct {
	// 队列内部指针
	woffset int64 // 写偏移量 记录当前队列已生成消息到哪个位置
	roffset int64 // 读偏移量 记录当前队列已消费消息到哪个位置
	soffset int64 // 扫描偏移量 用于处理过期消息

	// 队列属性
	filesize int64    // 当前队列文件的大小
	num      int64    // 队列中的消息数量 包括 未消费 和 待确认 两种
	name     string   // 队列名称 由 topic.name 和  queue.bindKey 两部分组成
	data     []byte   // 内存映射文件
	topic    *Topic   // 所属topic
	file     *os.File // 队列文件指针
	ctx      *Context

	// TODO 一些队列可靠性与特性的实现机制
	bindKey  string           // 绑定键 topic.name_queue.bindKey 是队列的唯一标识
	waitAck  map[uint64]int64 // 等待确认消息 消息ID 和 消息位置的映射关系
	readChan chan *readQueueData
	closed   bool
	wg       utils.WaitGroupWrapper

	exitChan          chan struct{}
	notifyReadMsgChan chan bool
	sync.RWMutex
}

type readQueueData struct {
	data []byte
	pos  int64
}

// 新建一个队列
func NewQueue(name, bindKey string, ctx *Context, topic *Topic) *queue {
	queue := &queue{
		name:     name,
		ctx:      ctx,
		topic:    topic,
		bindKey:  bindKey,
		waitAck:  make(map[uint64]int64),
		readChan: make(chan *readQueueData),
		exitChan: make(chan struct{}),

		notifyReadMsgChan: make(chan bool),
	}

	path := fmt.Sprintf("%s/%s.queue", ctx.Conf.DataSavePath, name)
	// 新建/打开 队列文件
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatalf("open %s.queue failed, %v\n", queue.name, err)
	} else {
		queue.file = f
	}

	stat, err := f.Stat()
	if err != nil {
		log.Fatalf("get %s.queue stat failed, %v\n", queue.name, err)
	}

	// Mmap(file, size) 中的 size 的初始值
	initMmapSize := int(stat.Size())

	// 初识文件的文件大小 == 4096 bytes
	if initMmapSize == 0 {
		if _, err := f.WriteAt([]byte{'0'}, int64(os.Getpagesize())-1); err != nil {
			log.Fatalf("sync %v.queue failed, %v\n", queue.name, err)
		}
		if err := f.Sync(); err != nil {
			log.Fatalf("sync %v.queue failed, %v\n", queue.name, err)
		}

		initMmapSize = os.Getpagesize()
	}

	// 进行内存映射 queue.data 中就有了 queue.file 的内容
	if err := queue.mmap(initMmapSize); err != nil {
		log.Fatalln(err)
	}

	queue.wg.Wrap(queue.loopRead)
	return queue

}

// 循环读
func (q *queue) loopRead() {
	for {
		queueData, err := q.read(q.topic.isAutoAck)
		if err != nil {
			q.LogError(err)
		}

		select {
		case <-q.exitChan:
			return
		case q.readChan <- queueData:
		}
	}
}

// 通知 queue 中运行的goroutine 们退出 已收录到 破大防 中
func (q *queue) exit() {
	q.closed = true
	close(q.exitChan)
	close(q.notifyReadMsgChan)
	q.wg.Wait() // 上面的操作是通知 goroutine 需要退出 等所有goroutine都退出后 再exit
}

// scan 里面有递归遍历队列 值得借鉴
func (q *queue) scan() ([]byte, error) {
	if q.closed {
		return nil, ErrQueueClosed
	}

	q.Lock()
	if q.soffset > REWRITE_SIZE {
		if err := q.rewrite(); err != nil {
			q.LogError(err)
		}
	}

	if q.soffset == q.roffset {
		q.Unlock()
		return nil, ErrMessageNotExist
	}

	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	// 读取 flag status 和 msgLen
	if flag := q.data[q.soffset]; flag != 'v' {
		q.Unlock()
		return nil, errors.New("unkown msg flag")
	}
	status := binary.BigEndian.Uint16(q.data[q.soffset+1 : q.soffset+3])
	msgLen := int64(binary.BigEndian.Uint32(q.data[q.soffset+3 : q.soffset+7]))

	if status == MSG_STATUS_READ {
		q.Unlock()
		return nil, ErrMessageNotExpire
	}

	// scan next message when the current message is finish 使用递归
	if status == MSG_STATUS_FIN {
		q.soffset += MSG_FIX_LENGTH + msgLen
		atomic.AddInt64(&q.num, -1)
		q.Unlock()
		return q.scan()
	}
	// TODO 消息过期机制
	expireTime := binary.BigEndian.Uint64(q.data[q.soffset+7 : q.soffset+15])
	q.LogDebug(fmt.Sprintf("msg.expire:%v now:%v", expireTime, time.Now().Unix()))

	// has not expire message
	if expireTime > uint64(time.Now().Unix()) {
		q.Unlock()
		return nil, ErrMessageNotExpire
	}

	// message will be consumed if it is expired 如果消息过期了 直接消费
	binary.BigEndian.PutUint16(q.data[q.soffset+1:q.soffset+3], uint16(MSG_STATUS_EXPIRE))
	msg := make([]byte, msgLen)
	copy(msg, q.data[q.soffset+7:q.soffset+7+msgLen])
	q.soffset += MSG_FIX_LENGTH + msgLen
	atomic.AddInt64(&q.num, -1)

	q.Unlock()
	return msg, nil
}

// 重写文件大小 注意这个函数需要先上锁 再调用 里面有操作 map
func (q *queue) rewrite() error {
	q.LogInfo(fmt.Sprintf("begin rewrite %v.queue, filesize:%v.", q.name, q.filesize))
	defer func() {
		q.LogInfo(fmt.Sprintf("after rewrite %v.queue, filesize:%v.", q.name, q.filesize))
	}()

	// 临时文件 迁移完 这个文件会替代原来的队列文件
	tempPath := fmt.Sprintf("%s/%s.temp.queue", q.ctx.Conf.DataSavePath, q.name)
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	pageSize := os.Getpagesize()
	size := q.filesize - q.soffset
	q.LogDebug(fmt.Sprintf("fileszie:%v,queue.soffset:%v,size:%v", q.filesize, q.soffset, size))

	// 确保 mmap 大小是页面大小的倍数
	sz := int(size)
	if (sz % pageSize) != 0 {
		remainSize := int64(sz - sz/pageSize*pageSize)
		if (q.filesize-q.woffset)-remainSize-int64(pageSize) > 0 {
			// 文件剩余空间满足页的大小的倍数,继续缩小文件大小
			size = size - remainSize
		} else {
			// 不满足,需要增加文件大小
			size = int64(sz/pageSize+1) * int64(pageSize)
		}
	}

	/* 神奇的申请内存方式 mmap */
	/* 先扩展映射文件的大小到需要的尺寸 */
	/* 然后调用 mmap file -> []byte 然后 []byte 里面的内容会被写到文件里 */

	// 扩展文件
	_, err = f.WriteAt([]byte{'0'}, size-1)
	if err != nil {
		return err
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	// 将旧文件剩余数据迁移到新文件上
	copy(data, q.data[q.soffset:])

	// 解除之前内存映射
	q.file.Close()
	if err := q.unmap(); err != nil {
		return err
	}
	// 重新设置 queue 的一些属性
	q.LogDebug(fmt.Sprintf("before rewrite, scan-offset:%v, read-offset:%v, write-offset:%v",
		q.soffset, q.roffset, q.woffset))
	q.data = data
	q.file = f
	q.roffset -= q.soffset
	q.woffset -= q.soffset
	q.filesize = size
	q.LogDebug(fmt.Sprintf("after rewrite, scan-offset:%v, read-offset:%v, write-offset:%v",
		q.soffset, q.roffset, q.woffset))

	// 遍历 queue 的所有等待确认的消息(通过ID) 将每一条待确认消息的长度 减 q.soffset 表示 之前的消息已经全部消费过了
	for k, v := range q.waitAck {
		q.waitAck[k] = v - q.soffset
	}

	q.soffset = 0 // 由于新文件中只有没有扫描的消息 所以 soffset 置0
	path := fmt.Sprintf("%s/%s.queue", q.ctx.Conf.DataSavePath, q.name)
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	return nil
}

// 消息设置为已确认消费
func (q *queue) ack(msgId uint64) error {
	if q.closed {
		return ErrQueueClosed
	}

	q.Lock()
	defer q.Unlock()

	offset, ok := q.waitAck[msgId]
	if !ok {
		return fmt.Errorf("msgId:%v is not exist.", msgId)
	}

	if offset > int64(len(q.data))-1 {
		return fmt.Errorf("ack.offset greather than queue.length.")
	}
	if q.data[offset] != 'v' {
		return fmt.Errorf("ack.offset error.")
	}

	binary.BigEndian.PutUint16(q.data[offset+1:offset+3], MSG_STATUS_FIN)
	delete(q.waitAck, msgId)
	return nil
}

// 移除消息等待状态
func (q *queue) removeWait(msgId uint64) error {
	q.Lock()
	defer q.Unlock()

	_, ok := q.waitAck[msgId]
	if !ok {
		return fmt.Errorf("msgId:%v is not exist.", msgId)
	}

	delete(q.waitAck, msgId)
	return nil
}

// 建立映射
func (q *queue) mmap(size int) error {
	stat, err := q.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap %v.queue failed, %v.\n", q.name, err)
	}
	if stat.Size() == 0 {
		return fmt.Errorf("mmap %v.queue failed, file is empty", q.name)
	}

	// 解除上次映射 如果有的话
	if len(q.data) > 0 {
		if err := q.unmap(); nil != err {
			return err
		}
	}

	if err := mmap(q, size); nil != err {
		return err
	}

	q.filesize = stat.Size()
	return nil
}

// 解除映射
func (q *queue) unmap() error {
	return unmap(q)
}

// 队列读取消息 调用一次 读取一条 如果队列中没有数据 会阻塞
func (q *queue) read(isAutoAck bool) (*readQueueData, error) {
	if q.closed {
		return nil, ErrQueueClosed
	}

	q.Lock()
	defer q.Unlock()

	msgOffset := q.roffset
	if q.roffset == q.woffset {
		q.Unlock()
		// 等待消息写入
		hasMsg := <-q.notifyReadMsgChan
		q.Lock()
		if !hasMsg {
			return nil, nil
		}
	}

	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	// msg又包括expire(8-bytes) + id(8-bytes) + retry(2-bytes) + body(n-bytes)
	if flag := q.data[q.roffset]; flag != 'v' {
		fmt.Println("xxxxx", string(flag), flag)
		return nil, errors.New("unkown msg flag")
	}

	msgLen := int64(binary.BigEndian.Uint32(q.data[q.roffset+3 : q.roffset+7]))
	msg := make([]byte, msgLen)
	copy(msg, q.data[q.roffset+7:q.roffset+7+msgLen])

	if isAutoAck {
		binary.BigEndian.PutUint16(q.data[q.roffset+1:q.roffset+3], uint16(MSG_STATUS_FIN))
		atomic.AddInt64(&q.num, -1)
	} else {
		binary.BigEndian.PutUint16(q.data[q.roffset+1:q.roffset+3], uint16(MSG_STATUS_WAIT))
		binary.BigEndian.PutUint64(q.data[q.roffset+7:q.roffset+15], uint64(time.Now().Unix())+uint64(q.topic.msgTTR))
		msgId := binary.BigEndian.Uint64(q.data[q.roffset+15 : q.roffset+23])
		q.waitAck[msgId] = msgOffset
	}

	q.roffset += MSG_FIX_LENGTH + msgLen
	return &readQueueData{msg, msgOffset}, nil
}

func (q *queue) updateMsgStatus(msgId uint64, offset int64, status int) {
	q.Lock()
	defer q.Unlock()

	queueMsgId := binary.BigEndian.Uint64(q.data[offset+15 : offset+23])
	if msgId != queueMsgId {
		q.LogError(fmt.Sprintf("invaild msgId, msgId %d, but queue.msg.id is %d", msgId, queueMsgId))
		return
	}

	if flag := q.data[offset]; flag != 'v' {
		q.LogError(fmt.Sprintf("invaild offset, offset : %s", offset))
		return
	}

	switch status {
	case MSG_STATUS_WAIT:
		q.waitAck[msgId] = offset
	case MSG_STATUS_FIN:
	default:
		q.LogError(fmt.Sprintf("invaild status %d", status))
		return
	}

	binary.BigEndian.PutUint16(q.data[offset+1:offset+3], uint16(status))
}

// 新写入信息的长度不能超过文件大小,超过则新建文件
func (q *queue) write(msg []byte) error {
	if q.closed {
		return ErrQueueClosed
	}

	q.Lock()
	defer q.Unlock()

	msgLen := int64(len(msg))
	if q.woffset+MSG_FIX_LENGTH+msgLen > q.filesize {
		// 文件大小不够,需要扩展文件
		if err := q.grow(); err != nil {
			return err
		}
	}

	// package = flag(1-byte) + status(2-bytes) + msgLen(4-bytes) + msg(n-bytes)
	copy(q.data[q.woffset:q.woffset+1], []byte{'v'})
	binary.BigEndian.PutUint16(q.data[q.woffset+1:q.woffset+3], uint16(MSG_STATUS_DEFAULT))
	binary.BigEndian.PutUint32(q.data[q.woffset+3:q.woffset+7], uint32(msgLen))
	copy(q.data[q.woffset+7:q.woffset+7+msgLen], msg)

	q.woffset += MSG_FIX_LENGTH + msgLen
	atomic.AddInt64(&q.num, 1)

	// 通知消费者已有消息，如果没有消费者等待notifyReadMsgChan
	// default分支用于保证不会阻塞生产者生产消息
	select {
	case q.notifyReadMsgChan <- true:
	default:
	}

	return nil
}

// 扩展文件大小,每次为GROW_SIZE
func (q *queue) grow() error {
	// 重新映射
	fz := q.filesize + GROW_SIZE
	if err := q.mmap(int(fz)); err != nil {
		return err
	}

	if err := q.file.Sync(); err != nil {
		return errors.New(fmt.Sprintf("file sync error: %s\n", err))
	}

	q.LogInfo(fmt.Sprintf("grow %v.queue size to %v, and old is %v, default to %v",
		q.name, fz, q.filesize, GROW_SIZE))
	q.filesize = fz
	return nil
}

func (q *queue) LogError(msg interface{}) {
	q.ctx.Logger.Error(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogWarn(msg interface{}) {
	q.ctx.Logger.Warn(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogInfo(msg interface{}) {
	q.ctx.Logger.Info(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogDebug(msg interface{}) {
	q.ctx.Logger.Debug(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogTrace(msg interface{}) {
	q.ctx.Logger.Trace(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}
