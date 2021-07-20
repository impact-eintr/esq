package main

import (
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/impact-eintr/esq/esqd"
	"github.com/impact-eintr/esq/internal/lg"
	"github.com/judwhite/go-svc"
)

type program struct {
	once sync.Once
	esqd *esqd.ESQD
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	return nil
}

func (p *program) Start() error {
	opts := esqd.NewOptions()

	flagSet := esqFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.esqd.Exit()
	})
	return nil

}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[esqd]", f, args...)
}
