// 日志调度器
// 功能：
// 初始化各个日志处理器
package logs

import "fmt"

type logTarget struct {
	name   string
	hander target
}

// 调度器对象
type Dispatcher struct {
	Level   int
	Targets []*logTarget
}

// 处理器对象关系表
var targetMap = make(map[string]target)

// 注册适配器
func RegisterTareget(name string, tg target) {
	if _, ok := targetMap[name]; ok {
		panic("适配器已经被注册过")
	} else {
		targetMap[name] = tg
	}
}

// 新建调度器
func NewDispatcher(lvl int) *Dispatcher {
	return &Dispatcher{
		Level: lvl,
	}
}

// 设置日志处理对象(TAGET_FILE TAGET_CONSOLE)
func (d *Dispatcher) SetTarget(name string, config string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	target, ok := targetMap[name]
	if !ok {
		panic("适配器未注册")
	}

	// 不能重复设置同一个对象
	for _, tg := range d.Targets {
		if name == tg.name {
			panic(fmt.Errorf("Can not set target %s twice \n", name))
		}
	}
	// 初始化提供者配置
	if err := target.Init(config); err != nil {
		panic("初始化提供这配置失败：" + err.Error())
	}

	d.Targets = append(d.Targets, &logTarget{
		name:   name,
		hander: target,
	})
}

// 错误输出
func (d *Dispatcher) Error(msg ...interface{}) {
	if d.Level < LOG_ERROR {
		return
	}

	logData := wrapLogData(LOG_ERROR, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 追踪日志
func (d *Dispatcher) Trace(msg ...interface{}) {
	if d.Level < LOG_TRACE {
		return
	}

	logData := wrapLogData(LOG_TRACE, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 调试输出
func (d *Dispatcher) Debug(msg ...interface{}) {
	if d.Level < LOG_DEBUG {
		return
	}

	logData := wrapLogData(LOG_DEBUG, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 信息输出
func (d *Dispatcher) Info(msg ...interface{}) {
	if d.Level < LOG_INFO {
		return
	}

	logData := wrapLogData(LOG_INFO, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}

// 警告输出
func (d *Dispatcher) Warn(msg ...interface{}) {
	if d.Level < LOG_WARN {
		return
	}

	logData := wrapLogData(LOG_WARN, msg...)
	for _, tg := range d.Targets {
		tg.hander.WriteMsg(logData)
	}
}
