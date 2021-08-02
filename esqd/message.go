package esqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDSize       = 16
	minValidMsgSize = MsgIDSize + 8 + 2 // timestamp + attempts(尝试)
)

type MessageID [MsgIDSize]byte

type Message struct {
	ID        MessageID // 消息ID
	Body      []byte    // 消息体
	TimeStamp int64     // 产生的时间戳
	Attempts  uint16    // 消息尝试发送的的次数

	deliveryTS time.Time     // 发送的时间 delivery 交付
	clientID   int64         // 被消费的consume
	pri        int64         // 消息的timout时间
	index      int           // 在queue中的index
	deferred   time.Duration // 消息的延迟发送时间
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		TimeStamp: time.Now().UnixNano(),
	}
}

// 写入bytes.buffer
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.TimeStamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}
	return total, nil

}

// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgSize {
		return nil, fmt.Errorf("invalid messaeg buffer size", len(b))
	}

	msg.TimeStamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDSize])
	msg.Body = b[10+MsgIDSize:]

	return &msg, nil

}

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())

}
