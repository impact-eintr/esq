package esqd

import "github.com/impact-eintr/esq/internal/lg"

type Logger lg.Logger

const (
	LOG_DEBUG = lg.DEBUG
	LOG_INFO  = lg.INFO
	LOG_WARN  = lg.WARN
	LOG_ERROR = lg.ERROR
	LOG_FATAL = lg.FATAL
)

func (e *ESQD) logf(level lg.LogLevel, f string, args ...interface{}) {
	opts := e.getOpts()
	lg.Logf(opts.Logger, opts.LogLevel, level, f, args...)
}
