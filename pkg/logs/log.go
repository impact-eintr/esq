// 日志模块
// 采用设计模式:适配器模式
// 参与者:日志调度器(Dispatcher),日志具体处理器(Target,such as fileTarget, consoleTarget)

// 使用:
// logger := logs.NewDispatcher()
// logger.SetTarget(logs.TARGET_FILE, `{"filename":"xxxx.log","level":10,"max_size":500,"rotate":true}`)
// logger.Error("这是一个错误")
// logger.Debug("这是一个调试")
// logger.Info("这是一个信息")
// logger.Warn("这是一个警告")
package logs

import "fmt"

// 日志等级
const (
	LOG_ERROR = iota
	LOG_WARN
	LOG_INFO
	LOG_TRACE
	LOG_DEBUG
)

// 日志落地标志
const (
	TARGET_FILE    = "file"
	TARGET_CONSOLE = "console"
)

// 日志前缀
var logPrefixMap = map[int]string{
	LOG_ERROR: "E",
	LOG_WARN:  "W",
	LOG_DEBUG: "D",
	LOG_INFO:  "I",
	LOG_TRACE: "T",
}

type logData struct {
	category string // 日志种类
	msg      string // 消息内容
	level    int    // 日志等级
}

type LogCategory string

// 格式化输出日志
func wrapLogData(level int, msg ...interface{}) (data logData) {
	data = logData{
		level:    level,
		msg:      "",
		category: "default",
	}

	for _, v := range msg {
		switch v.(type) {
		case LogCategory:
			data.category = fmt.Sprint(v)
		default:
			data.msg += fmt.Sprintf("%v ", v)
		}
	}

	return
}
