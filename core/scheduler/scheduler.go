package scheduler

import (
	"context"
	"errors"
	"fmt"
	"mini-ray/common"
	"sync"
	"time"

	pb "mini-ray/proto"

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

type Scheduler struct {
	lg            *zap.Logger
	pool          *ants.Pool
	listenAddr    string
	advertiseAddr string
	started       abool.AtomicBool
	options       common.Options
	connMgr       *connectionManager
	workerConns   map[string]common.Connection
	gcsConns      map[string]common.Connection

	taskChan     chan common.Task
	pendingTasks sync.Map
}

func NewScheduler(ctx context.Context, lg *zap.Logger, pool *ants.Pool, listenAddr, advertiseAddr string, opts ...common.Option) *Scheduler {
	options := common.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	s := &Scheduler{
		lg:            lg,
		pool:          pool,
		listenAddr:    listenAddr,
		advertiseAddr: advertiseAddr,
		options:       options,
		taskChan:      make(chan common.Task, options.TaskChanSize),
		workerConns:   make(map[string]common.Connection),
		gcsConns:      make(map[string]common.Connection),
	}
	s.connMgr = NewConnectionManager(ctx, lg, advertiseAddr, s)
	return s
}

func (s *Scheduler) Start() {
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

func (s *Scheduler) Loop() {
	// TODO:consider load balance
	for {
		if len(s.taskChan) > 0 {
			s.AssignTask()
		}
		time.Sleep(time.Second * 1)
	}
}

func (s *Scheduler) Connect(ctx context.Context, addr string) {
	s.connMgr.AddConnection(ctx, addr)
}

func (s *Scheduler) PushTask(task common.Task) error {
	select {
	case s.taskChan <- task:
		s.lg.Info("Task pushed to channel ", zap.String("task id", task.ID))
		return nil
	default:
		return errors.New("task channel is full")
	}
}

func (s *Scheduler) AssignTask() error {
	var task common.Task
	select {
	case task = <-s.taskChan:
	default:
		return errors.New("task channel is empty")
	}

	// scheduler strategy
	//s.workerConns
	for _, conn := range s.workerConns {
		var req pb.AssignTaskRequest
		req.TaskId = task.ID
		req.FuncName = task.FuncName
		for _, arg := range task.Args {
			req.Args = append(req.Args, arg)
		}
		s.pendingTasks.Store(task.ID, task)
		s.lg.Info("Task start pending ", zap.String("task id", task.ID))
		s.connMgr.handleServerReq(conn, &req)
	}
	return nil
}
