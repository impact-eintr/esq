package gnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type HttpApi struct {
	ctx *Context
}

type topicData struct {
	Name        string `json:"name"`
	PopNum      int64  `json:"pop_num"`
	PushNum     int64  `json:"push_num"`
	DeadNum     int64  `json:"dead_num"`
	QueueMsgNum int64  `json:"queue_msg_num"`
	QueueNum    int    `json:"queue_num"`
	DelayNum    int    `json:"delay_num"`
	WaitAckNum  int    `json:"wait_ack_num"`
	StartTime   string `json:"start_time"`
	IsAutoAck   bool   `json:"is_auto_ack"`
	IsMultiple  bool   `json:"is_multiple"`
}

// curl "http://127.0.0.1:9504/pop?topic=xxx&bindKey=xxx"
// 消费任务
func (h *HttpApi) Pop(c *gin.Context) {
	topic := Get(c, "topic")
	if len(topic) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}
	bindKey := Get(c, "bindKey")
	if len(bindKey) == 0 {
		JsonErr(c, errors.New("bindKey is empty"))
		return
	}

	msg, err := h.ctx.Dispatcher.pop(topic, bindKey)
	if err != nil {
		JsonErr(c, err)
		msg = nil
		return
	}

	data := RespMsgData{
		Id:    strconv.FormatUint(msg.Id, 10),
		Body:  string(msg.Body),
		Retry: msg.Retry,
	}

	JsonData(c, data)
	msg = nil
	return

}

// curl "http://127.0.0.1:9504/declareQueue?topic=xxx&bindKey=kkk"
// 声明队列
func (h *HttpApi) DeclareQueue(c *gin.Context) {
	topic := Get(c, "topic")
	if len(topic) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}
	bindKey := Get(c, "bindKey")
	if len(bindKey) == 0 {
		JsonErr(c, errors.New("bindKey is empty"))
		return
	}

	if err := h.ctx.Dispatcher.declareQueue(topic, bindKey); err != nil {
		JsonErr(c, err)
		return
	}

	JsonSuccess(c, "ok")

}

// curl http://127.0.0.1:9504/push -X POST -d 'data={"body":"this is a job","topic":"xxx","delay":20,"route_key":"xxx"}'
// 推送消息
func (h *HttpApi) Push(c *gin.Context) {
	data := Post(c, "data")
	if len(data) == 0 {
		JsonErr(c, errors.New("data is empty"))
		return
	}

	msg := RecvMsgData{}
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		JsonErr(c, err)
		return
	}

	msgId, err := h.ctx.Dispatcher.push(msg.Topic, msg.RouteKey, []byte(msg.Body), msg.Delay)
	if err != nil {
		JsonErr(c, err)
		return
	}

	var rsp = make(map[string]string)
	rsp["msgId"] = strconv.FormatUint(msgId, 10)
	JsonData(c, rsp)

}

// curl http://127.0.0.1:9504/ack?msgId=xxx&topic=xxx&bindKey=xxx
func (h *HttpApi) Ack(c *gin.Context) {
	msgId := GetInt64(c, "msgId")
	if msgId == 0 {
		JsonErr(c, errors.New("msgId is empty"))
		return
	}
	topic := Get(c, "topic")
	if len(topic) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}
	bindKey := Get(c, "bindKey")
	if len(bindKey) == 0 {
		JsonErr(c, errors.New("bindKey is empty"))
		return
	}

	if err := h.ctx.Dispatcher.ack(topic, uint64(msgId), bindKey); err != nil {
		JsonErr(c, err)
	} else {
		JsonSuccess(c, "success")
	}

}

// curl "http://127.0.0.1:9504/config?topic=xxx&isAuthoAck=1&mode=1&msgTTR=30&msgRetry=5"
// 配置topic
func (h *HttpApi) Config(c *gin.Context) {
	topic := Get(c, "topic")
	if len(topic) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}

	configure := &topicConfigure{
		isAutoAck: GetInt(c, "isAutoAck"),
		//isMultiple: GetInt(c, "isMultiple"),
		mode:     GetInt(c, "mode"),
		msgTTR:   GetInt(c, "msgTTR"),
		msgRetry: GetInt(c, "msgRetry"),
	}

	err := h.ctx.Dispatcher.set(topic, configure)
	configure = nil
	if err != nil {
		JsonErr(c, err)
	} else {
		JsonSuccess(c, "success")
	}

}

// 获取指定topic统计信息
// curl "http://127.0.0.1:9504/getTopicStat?topic=ketang"
func (h *HttpApi) GetTopicStat(c *gin.Context) {
	name := Get(c, "topic")
	if len(name) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}

	t, err := h.ctx.Dispatcher.GetExistTopic(name)
	if err != nil {
		JsonErr(c, err)
		return
	}

	data := topicData{}
	data.Name = t.name
	data.QueueNum = len(t.queues)
	data.PopNum = t.popNum
	data.PushNum = t.pushNum
	data.DeadNum = t.deadNum
	data.DelayNum = t.getBucketNum()
	data.IsAutoAck = t.isAutoAck
	data.IsMultiple = t.isMultiple
	data.StartTime = t.startTime.Format("2006-01-02 15:04:05")

	for _, q := range t.queues {
		data.WaitAckNum += len(q.waitAck)
		data.QueueMsgNum += q.num
	}

	JsonData(c, data)
}

// 获取所有topic统计信息
// curl http://127.0.0.1:9504/getAllTopicStat
// http://127.0.0.1:9504/getAllTopicStat
func (h *HttpApi) GetAllTopicStat(c *gin.Context) {
	topics := h.ctx.Dispatcher.GetTopics()

	var topicDatas = make([]topicData, len(topics))
	for i, t := range topics {
		data := topicData{}
		data.Name = t.name
		data.PopNum = t.popNum
		data.PushNum = t.pushNum
		data.DeadNum = t.deadNum
		data.QueueNum = len(t.queues)
		data.DelayNum = t.getBucketNum()
		data.IsAutoAck = t.isAutoAck
		data.StartTime = t.startTime.Format("2006-01-02 15:04:05")

		for _, q := range t.queues {
			data.WaitAckNum += len(q.waitAck)
			data.QueueMsgNum += q.num
		}

		topicDatas[i] = data
	}

	JsonData(c, topicDatas)

}

// 退出topic
// curl http://127.0.0.1:9504/exitTopic?topic=xxx
// http://127.0.0.1:9504/exitTopic?topic=xxx
func (h *HttpApi) ExitTopic(c *gin.Context) {
	name := Get(c, "topic")
	if len(name) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}

	// topic不存在或没有被客户端请求
	topic, err := h.ctx.Dispatcher.GetExistTopic(name)
	if err != nil {
		JsonErr(c, err)
		return
	}

	topic.exit()
	topic = nil
	delete(h.ctx.Dispatcher.topics, name)
	JsonSuccess(c, fmt.Sprintf("topic.%s has exit.", name))

}

// 设置主题自动确认消息
// curl http://127.0.0.1:9504/setIsAutoAck?topic=xxx
// http://127.0.0.1:9504/setIsAutoAck?topic=xxx
func (h *HttpApi) SetIsAutoAck(c *gin.Context) {
	name := Get(c, "topic")
	if len(name) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}

	// topic不存在或没有被客户端请求
	topic, err := h.ctx.Dispatcher.GetExistTopic(name)
	if err != nil {
		JsonErr(c, err)
		return
	}

	var vv int
	v := topic.isAutoAck
	if v {
		vv = 0
	} else {
		vv = 1
	}

	configure := &topicConfigure{
		isAutoAck: vv,
	}

	if err := topic.set(configure); err != nil {
		configure = nil
		JsonErr(c, err)
		return
	}

	configure = nil
	JsonSuccess(c, "success")
}

func (h *HttpApi) GetQueuesByTopic(c *gin.Context) {
	name := Get(c, "topic")
	if len(name) == 0 {
		JsonErr(c, errors.New("topic is empty"))
		return
	}

	data := make(map[string]string)
	topic := h.ctx.Dispatcher.GetTopic(name)
	for k, v := range topic.queues {
		data[k] = v.name
	}

	JsonData(c, data)

}

// 心跳接口
func (h *HttpApi) Ping(c *gin.Context) {
	c.Writer.WriteHeader(http.StatusOK)
	c.Writer.Write([]byte{'O', 'K'})
}
