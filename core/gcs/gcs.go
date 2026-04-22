package gcs

import (
	"context"
	"fmt"
	"time"

	"mini-ray/common"

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

type ObjectID string

type GCS struct {
	ctx               context.Context
	lg                *zap.Logger
	pool              *ants.Pool
	listenAddr        string
	advertiseAddr     string
	started           abool.AtomicBool
	options           common.Options
	connMgr           *connectionManager
	objectLocationMap map[ObjectID]string
}

func NewGCS(ctx context.Context, lg *zap.Logger, workerPool *ants.Pool, listenAddr string, advertiseAddr string, opts ...common.Option) *GCS {
	options := common.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	newGCS := &GCS{
		ctx:               ctx,
		lg:                lg,
		pool:              workerPool,
		listenAddr:        listenAddr,
		advertiseAddr:     advertiseAddr,
		objectLocationMap: make(map[ObjectID]string),
		options:           options,
	}
	mgr := NewConnectionManager(ctx, lg, advertiseAddr, newGCS)
	newGCS.connMgr = mgr

	return newGCS
}

func (s *GCS) Connect(ctx context.Context, addr string) {
	s.connMgr.AddConnection(ctx, addr)
}

func (s *GCS) Start() {
	if s.started.SetToIf(false, true) {
		s.pool.Submit(func() {
			gopts := []gnet.Option{
				gnet.WithMulticore(false),
				gnet.WithLogLevel(zapcore.DebugLevel),
				gnet.WithTCPKeepAlive(time.Minute * 5),
				gnet.WithSocketRecvBuffer(defaultSocketBufferSize),
				gnet.WithSocketSendBuffer(defaultSocketBufferSize),
				gnet.WithReuseAddr(s.options.ReuseAddr),
				gnet.WithReusePort(s.options.ReusePort),
				gnet.WithWriteBufferCap(defaultWriteBufferCap),
			}
			err := gnet.Run(s.connMgr, fmt.Sprintf("tcp://%s", s.listenAddr), gopts...)
			if err != nil {
				s.lg.Panic("Failed to start gnet", zap.Error(err))
			}
		})
	}
}

func (s *GCS) Loop() {
	s.lg.Info("gcs loop started", zap.String("listen", s.listenAddr), zap.String("advertise", s.advertiseAddr))
	for {
		ticker := time.NewTicker(1 * time.Second)
		select {
		case <-ticker.C:
			s.connMgr.Ontick()
		}
	}
}
