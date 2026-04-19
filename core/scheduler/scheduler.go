package driver

import (
	"context"
	"mini-ray/common"

	"github.com/panjf2000/ants/v2"
	"github.com/tevino/abool/v2"
	"go.uber.org/zap"
)

type Scheduler struct {
	lg            *zap.Logger
	pool          *ants.Pool
	listenAddr    string
	advertiseAddr string
	started       abool.AtomicBool
	options       common.Options
	connMgr       *connectionManager
}

func NewScheduler(ctx context.Context, lg *zap.Logger, pool *ants.Pool, listenAddr, advertiseAddr string, opts ...common.Option) *Scheduler {
	newScheduler := NewScheduler(ctx, lg, pool, listenAddr, advertiseAddr, opts...)
	mgr := NewConnectionManager(lg, advertiseAddr, newScheduler)
	newScheduler.connMgr = mgr
	return newScheduler
}

func (s *Scheduler) Start() {}

func (s *Scheduler) Loop() {}
