package driver

import (
	"context"
	"mini-ray/common"
	pb "mini-ray/proto"

	"github.com/panjf2000/ants/v2"
	"github.com/tevino/abool/v2"
	"go.uber.org/zap"
)

type Driver struct {
	lg             *zap.Logger
	pool           *ants.Pool
	listenAddr     string
	advertiseAddr  string
	started        abool.AtomicBool
	options        common.Options
	taskNotifyChan map[string]chan string
	connMgr        *connectionManager
}

func NewDriver(ctx context.Context, lg *zap.Logger, pool *ants.Pool, listenAddr, advertiseAddr string, opts ...common.Option) *Driver {
	newDriver := NewDriver(ctx, lg, pool, listenAddr, advertiseAddr, opts...)
	mgr := NewConnectionManager(lg, advertiseAddr, newDriver)
	newDriver.connMgr = mgr
	newDriver.taskNotifyChan = make(map[string]chan string)
	return newDriver
}

func (d *Driver) Start() {}

func (d *Driver) Loop() {}

func (d *Driver) ConnectToScheduler(ctx context.Context, addr string) {
	d.connMgr.AddConnection(ctx, addr)
}

func (d *Driver) ConnectToGCS(ctx context.Context, addr string) {
	d.connMgr.AddConnection(ctx, addr)
}

func (d *Driver) SubmitTask(ctx context.Context, task *pb.DriverSubmitTaskRequest) {
	d.connMgr.SubmitTask(ctx, task)
}

func (d *Driver) onSchedulerSubmitTaskResp(resp *pb.SchedulerSubmitTaskResponse) {
	d.lg.Debug("onSchedulerSubmitTaskResp", zap.String("resp", resp.String()))
	if resp.Success {
		d.lg.Debug("submit task success", zap.String("task", resp.TaskId))
	} else {
		d.lg.Debug("submit task failed", zap.String("task", resp.TaskId))
	}
	select {
	case d.taskNotifyChan[resp.TaskId] <- common.SubmitTaskSuccess:
	default:
	}
}
