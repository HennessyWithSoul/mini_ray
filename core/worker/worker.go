package worker

import (
	"context"
	"fmt"
	"mini-ray/common"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/tevino/abool/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultSocketBufferSize = 2 * 1024 * 1024
	defaultWriteBufferCap   = 64 * 1024
)

type worker struct {
	lg            *zap.Logger
	pool          *ants.Pool
	listenAddr    string
	advertiseAddr string
	started       abool.AtomicBool
	options       common.Options
	connMgr       *connectionManager
}

func NewWorker(ctx context.Context, lg *zap.Logger, pool *ants.Pool, listenAddr, advertiseAddr string, opts ...common.Option) *worker {
	options := common.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	w := &worker{
		lg:            lg,
		pool:          pool,
		listenAddr:    listenAddr,
		advertiseAddr: advertiseAddr,
		options:       options,
	}
	w.connMgr = NewConnectionManager(ctx, lg, advertiseAddr, w)
	return w
}

func (w *worker) Start() {
	if w.started.SetToIf(false, true) {
		w.pool.Submit(func() {
			gopts := []gnet.Option{
				gnet.WithMulticore(false),
				gnet.WithLogLevel(zapcore.DebugLevel),
				gnet.WithTCPKeepAlive(time.Minute * 5),
				gnet.WithSocketRecvBuffer(defaultSocketBufferSize),
				gnet.WithSocketSendBuffer(defaultSocketBufferSize),
				gnet.WithReuseAddr(w.options.ReuseAddr),
				gnet.WithReusePort(w.options.ReusePort),
				gnet.WithWriteBufferCap(defaultWriteBufferCap),
			}
			err := gnet.Run(w.connMgr, fmt.Sprintf("tcp://%s", w.listenAddr), gopts...)
			if err != nil {
				w.lg.Panic("Failed to start gnet", zap.Error(err))
			}
		})
	}
}

func (w *worker) Loop() {
	//w.started.SetTo(true)
}

func (w *worker) Connect(ctx context.Context, addr string) {
	w.connMgr.AddConnection(ctx, addr)
}

func (w *worker) onAddTowInt(conn common.Connection, task common.Task) error {
	w.lg.Debug("onAddTowInt", zap.String("remote", conn.Remote()))
	_, err := w.AddTowIntFunc(task)
	//TODO object stroage
	if err != nil {
		return err
	}
	return nil
}
