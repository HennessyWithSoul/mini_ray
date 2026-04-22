package test

import (
	"context"
	"mini-ray/common"
	"mini-ray/core/gcs"
	scheduler "mini-ray/core/scheduler"
	workermod "mini-ray/core/worker"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

func TestSampleObjectTest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg, _ := zap.NewDevelopment()

	pool, err := ants.NewPool(8)
	if err != nil {
		t.Fatalf("ants pool: %v", err)
	}
	defer pool.Release()

	addrScheduler := freeTCPAddr(t)
	addrWorker := freeTCPAddr(t)
	addrGCS := freeTCPAddr(t)
	optsScheduler := []common.Option{
		common.WithTaskChanSize(100),
	}
	scheduler := scheduler.NewScheduler(ctx, lg, pool, addrScheduler, addrScheduler, optsScheduler...)
	w := workermod.NewWorker(ctx, lg, pool, addrWorker, addrWorker, common.WithRegisterObjectChanSize(100))
	gcs := gcs.NewGCS(ctx, lg, pool, addrGCS, addrGCS)
	scheduler.Start()
	w.Start()
	gcs.Start()
	scheduler.Connect(ctx, addrWorker)
	gcs.Connect(ctx, addrScheduler)
	gcs.Connect(ctx, addrWorker)
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
