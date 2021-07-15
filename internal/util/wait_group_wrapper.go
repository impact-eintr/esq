package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

// 封装waitGroup
func (w *WaitGroupWrapper) Wrap(handler func()) {
	w.Add(1)
	go func() {
		handler()
		w.Done()
	}()
}
