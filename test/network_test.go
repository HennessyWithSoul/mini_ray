package test

import (
	"context"
	"net"
	"testing"
	"time"

	"mini-ray/common"
	scheduler "mini-ray/core/scheduler"
	workermod "mini-ray/core/worker"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate tcp port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
func TestWorkerToScheduler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg, _ := zap.NewDevelopment()

	pool, err := ants.NewPool(8)
	if err != nil {
		t.Fatalf("ants pool: %v", err)
	}
	defer pool.Release()

	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)

	scheduler := scheduler.NewScheduler(ctx, lg, pool, addrA, addrA)
	worker := workermod.NewWorker(ctx, lg, pool, addrB, addrB)

	scheduler.Start()
	worker.Start()

	worker.Connect(ctx, addrA)
	time.Sleep(1 * time.Second)
	go worker.Loop()

	time.Sleep(3 * time.Second)
}

func TestSchedulerAssignTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg, _ := zap.NewDevelopment()

	pool, err := ants.NewPool(8)
	if err != nil {
		t.Fatalf("ants pool: %v", err)
	}
	defer pool.Release()

	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)
	opts := []common.Option{
		common.WithTaskChanSize(100),
	}
	scheduler := scheduler.NewScheduler(ctx, lg, pool, addrA, addrA, opts...)
	w := workermod.NewWorker(ctx, lg, pool, addrB, addrB)

	scheduler.Start()
	w.Start()
	scheduler.Connect(ctx, addrB)
	go scheduler.Loop()
	time.Sleep(1 * time.Second)
	scheduler.PushTask(common.Task{
		ID:       "1",
		FuncName: "addTowInt",
		Args: [][]byte{
			workermod.EncodeInt(1),
			workermod.EncodeInt(2),
		},
		Dependencies: []string{},
	})

	time.Sleep(5 * time.Second)
}
