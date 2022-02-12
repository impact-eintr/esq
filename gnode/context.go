package gnode

import (
	"github.com/impact-eintr/esq/configs"
	"github.com/impact-eintr/esq/pkg/logs"
)

type Context struct {
	Gnode      *Gnode // 上下文保存的 节点对象
	Dispatcher *Dispatcher
	Conf       *configs.GnodeConfig // 配置
	Logger     *logs.Dispatcher     // 日志适配器
}
