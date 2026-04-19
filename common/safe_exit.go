package common

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Handler interface {
	SafeExit(s os.Signal)
}

var (
	handlerMtx sync.RWMutex
	handlers   = make([]Handler, 0)
)

func RegisterHandler(h Handler) {
	if h == nil {
		return
	}

	handlerMtx.Lock()
	defer handlerMtx.Unlock()
	handlers = append(handlers, h)
}

func callHandlers(s os.Signal) {
	handlerMtx.RLock()
	defer handlerMtx.RUnlock()
	for _, h := range handlers {
		h.SafeExit(s)
	}
}

func RegisterSignal(ctx context.Context, signals ...os.Signal) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, signals...)
	select {
	case <-ctx.Done():
		callHandlers(syscall.SIGHUP)
	case s := <-sigs:
		callHandlers(s)
	}
}
