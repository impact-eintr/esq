package gnode

import (
	"context"
	"encoding/json"
	"fmt"
	"impact-eintr/esq/pkg/logs"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	RESP_SUCCESS = iota // 响应成功
	RESP_FAILED         // 响应失败
)

type HttpServ struct {
	ctx *Context
}

func NewHttpServ(ctx *Context) *HttpServ {
	return &HttpServ{
		ctx: ctx,
	}
}

func (s *HttpServ) Run() {
	defer s.LogInfo("http server exit.")

	api := &HttpApi{
		ctx: s.ctx,
	}

	r := gin.New()
	r.Use(GinLogger(s))
	r.Use(GinRecovery(s, true))

	r.GET("/pop", api.Pop)
	r.POST("/push", api.Push)
	r.GET("/ack", api.Ack)
	r.GET("/ping", api.Ping)
	r.GET("/config", api.Config)
	r.GET("/multiple", api.Multiple)
	r.GET("/declareQueue", api.DeclareQueue)
	r.GET("/exitTopic", api.ExitTopic)
	r.GET("/setIsAutoAck", api.SetIsAutoAck)
	r.GET("/getTopicStat", api.GetTopicStat)
	r.GET("/getAllTopicStat", api.GetAllTopicStat)
	r.GET("/getQueuesByTopic", api.GetQueuesByTopic)

	addr := s.ctx.Conf.HttpServAddr
	serv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	processed := make(chan struct{})
	go func() {
		<-s.ctx.Gnode.exitChan
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := serv.Shutdown(ctx); err != nil {
			s.LogError(fmt.Sprintf("shutdown server failed, %s", err))
		}
		close(processed)
	}()

	var err error
	if s.ctx.Conf.HttpServEnableTls {
		certFile := s.ctx.Conf.HttpServCertFile
		keyFile := s.ctx.Conf.HttpServKeyFile
		s.LogInfo(fmt.Sprintf("http server(%s) is running with tls", addr))
		err = serv.ListenAndServeTLS(certFile, keyFile)
	} else {
		s.LogInfo(fmt.Sprintf("http server(%s) is running", addr))
		err = serv.ListenAndServe()
	}

	if err != nil {
		s.LogDebug(err)
		return
	}
}

func (s *HttpServ) LogError(msg interface{}) {
	s.ctx.Logger.Error(logs.LogCategory("HttpServer"), msg)
}

func (s *HttpServ) LogWarn(msg interface{}) {
	s.ctx.Logger.Warn(logs.LogCategory("HttpServer"), msg)
}

func (s *HttpServ) LogInfo(msg interface{}) {
	s.ctx.Logger.Info(logs.LogCategory("HttpServer"), msg)
}

func (s *HttpServ) LogDebug(msg interface{}) {
	s.ctx.Logger.Debug(logs.LogCategory("HttpServer"), msg)
}

// GinLogger 接收gin框架默认的日志
func GinLogger(s *HttpServ) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()

		infomsg := fmt.Sprintf("status: %d, method: %s, path: %s, query: %s, ip: %s, user-agent: %s, errors: %s, cost: %d",
			c.Writer.Status(), c.Request.Method,
			path, query, c.ClientIP(), c.Request.UserAgent(),
			c.Errors.ByType(gin.ErrorTypePrivate).String(), time.Since(start))
		s.LogInfo(infomsg)
	}
}

// GinRecovery recover掉项目可能出现的panic，并使用zap记录相关日志
func GinRecovery(s *HttpServ, stack bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Check for a broken connection, as it is not really a
				// condition that warrants a panic stack trace.
				var brokenPipe bool
				if ne, ok := err.(*net.OpError); ok {
					if se, ok := ne.Err.(*os.SyscallError); ok {
						if strings.Contains(strings.ToLower(se.Error()), "broken pipe") ||
							strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
							brokenPipe = true
						}
					}
				}

				httpRequest, _ := httputil.DumpRequest(c.Request, false)
				if brokenPipe {
					errmsg := fmt.Sprintf("%s %v %s", c.Request.URL.Path, err, string(httpRequest))
					s.LogError(errmsg)
					// If the connection is dead, we can't write a status to it.
					c.Error(err.(error)) // nolint: errcheck
					c.Abort()
					return
				}

				if stack {
					errmsg := fmt.Sprintf("[Recovery from panic]%v %s %s", err, string(httpRequest), string(debug.Stack()))
					s.LogError(errmsg)
				} else {
					errmsg := fmt.Sprintf("[Recovery from panic]%v %s", err, string(httpRequest))
					s.LogError(errmsg)
				}
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	}
}

func JsonData(c *gin.Context, data interface{}) {
	r := map[string]interface{}{
		"data": data,
		"msg":  "success",
		"code": 0,
	}

	outputJson(c.Writer, r)
}

func JsonMsg(c *gin.Context, code int, msg string) {
	r := map[string]interface{}{
		"data": nil,
		"msg":  msg,
		"code": code,
	}

	outputJson(c.Writer, r)
}

func JsonSuccess(c *gin.Context, msg string) {
	r := map[string]interface{}{
		"data": nil,
		"msg":  msg,
		"code": RESP_SUCCESS,
	}

	outputJson(c.Writer, r)
}

func JsonErr(c *gin.Context, err error) {
	r := map[string]interface{}{
		"data": nil,
		"msg":  err.Error(),
		"code": RESP_FAILED,
	}

	outputJson(c.Writer, r)
}

func outputJson(w http.ResponseWriter, data map[string]interface{}) {
	v, err := json.Marshal(data)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Header().Set("Content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(v)
	}
}

func Get(c *gin.Context, key string) string {
	return c.Request.URL.Query().Get(key)
}

func GetDefault(c *gin.Context, key string, def string) string {
	v := c.Request.URL.Query().Get(key)
	if len(v) == 0 {
		return def
	}
	return v
}

func GetInt(c *gin.Context, key string) int {
	v := c.Request.URL.Query().Get(key)
	iv, _ := strconv.Atoi(v) // ignore error
	return iv
}

func GetInt64(c *gin.Context, key string) int64 {
	v := c.Request.URL.Query().Get(key)
	iv, _ := strconv.ParseInt(v, 10, 64)
	return iv
}

func GetDefaultInt(c *gin.Context, key string, def int) int {
	v := c.Request.URL.Query().Get(key)
	iv, err := strconv.Atoi(v)
	if err != nil || iv == 0 {
		return def
	}
	return iv
}

func Post(c *gin.Context, key string) string {
	return c.Request.FormValue(key)
}
