package scheduler

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"mini-ray/codec"
	"mini-ray/common"
	pb "mini-ray/proto"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type connectionManager struct {
	gnet.BuiltinEventEngine
	ctx           context.Context
	lg            *zap.Logger
	advertiseAddr string
	srv           *Scheduler
	pool          *ants.Pool
	nextID        uint64

	conns sync.Map // uint64 -> *connection
}

func NewConnectionManager(ctx context.Context, lg *zap.Logger, advertiseAddr string, srv *Scheduler) *connectionManager {
	return &connectionManager{
		ctx:           ctx,
		lg:            lg,
		advertiseAddr: advertiseAddr,
		srv:           srv,
		pool:          srv.pool,
	}
}

func (cm *connectionManager) OnBoot(eng gnet.Engine) (action gnet.Action) {
	cm.lg.Info("gnet OnBoot", zap.String("advertise", cm.advertiseAddr))
	return gnet.None
}

func (cm *connectionManager) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	id := atomic.AddUint64(&cm.nextID, 1)
	remote := ""
	if ra := c.RemoteAddr(); ra != nil {
		remote = ra.String()
	}
	conn := newConnection(cm, cm.srv, id, remote, c, cm.lg)
	c.SetContext(conn)
	cm.conns.Store(id, conn)
	cm.lg.Debug("gnet OnOpen", zap.Uint64("id", id), zap.String("remote", remote))
	return nil, gnet.None
}

func (cm *connectionManager) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if v := c.Context(); v != nil {
		if conn, ok := v.(common.Connection); ok {
			cm.conns.Delete(conn.ID())
		}
	}
	return gnet.None
}

func (cm *connectionManager) OnTraffic(c gnet.Conn) (action gnet.Action) {
	if cm.srv != nil && cm.srv.started.IsNotSet() {
		return gnet.Shutdown
	}

	conn, ok := c.Context().(common.Connection)
	if !ok {
		return gnet.Close
	}

	for c.InboundBuffered() > 0 {
		header, payload, err := codec.ZeroCopyDecode(c)
		if err == io.ErrShortBuffer {
			return gnet.None
		}
		if err != nil {
			cm.lg.Error("decode", zap.String("from", conn.Remote()), zap.Error(err))
			return gnet.Close
		}
		if err = cm.OnPacket(conn, header, payload); err != nil {
			return gnet.Close
		}
	}
	return gnet.None
}

func (cm *connectionManager) AddConnection(ctx context.Context, addr string) {
	id := atomic.AddUint64(&cm.nextID, 1)
	conn := common.NewConnection(ctx, cm.lg, cm.pool, id, addr, cm, common.WithDialTimeout(common.ShakeHandTimeout))
	conn.Start()
}

func (cm *connectionManager) OnPacket(conn common.Connection, header *codec.Header, payload proto.Message) error {
	cm.lg.Debug("onPacket", zap.Uint16("uri", header.Uri))
	switch header.Uri {
	case codec.MetaEstablish:
		req, _ := payload.(*pb.EstablishReq)
		cm.lg.Debug("onEstablish", zap.String("remote", conn.Remote()), zap.String("addr", req.Addr))
		return cm.onEstablish(conn, header, req)
	case codec.MetaEstablishAck:
		cm.lg.Debug("onEstablishAck", zap.String("remote", conn.Remote()))
		req, _ := payload.(*pb.EstablishResponse)
		return cm.onEstablishAck(conn, header, req)
	case codec.MetaShakeHand:
		cm.lg.Debug("onShakeHand", zap.String("remote", conn.Remote()))
	case codec.RayAssignTaskResp:
		resp, _ := payload.(*pb.AssignTaskResponse)
		return cm.onAssignTaskResp(conn, header, resp)
	}
	return nil
}

func (cm *connectionManager) handleServerReq(conn common.Connection, req proto.Message) error {
	seqId := atomic.AddUint64(&cm.nextID, 1)
	switch req.(type) {
	case *pb.AssignTaskRequest:
		req := req.(*pb.AssignTaskRequest)
		header := &codec.Header{Uri: codec.RayAssignTask, SeqId: seqId}
		payload, err := codec.EncodePayload(header, req)
		if err != nil {
			return err
		}
		return conn.Send(payload)
	}
	return nil
}

func (cm *connectionManager) onEstablish(conn common.Connection, header *codec.Header, req *pb.EstablishReq) error {
	conn.SetEstablished()
	resp := &pb.EstablishResponse{Err: "OK", Mode: common.ModeScheduler}
	ack := &codec.Header{Uri: codec.MetaEstablishAck, SeqId: header.SeqId}
	payload, err := codec.EncodePayload(ack, resp)
	if err != nil {
		return err
	}
	return conn.Send(payload)
}

func (cm *connectionManager) onEstablishAck(conn common.Connection, header *codec.Header, resp *pb.EstablishResponse) error {
	cm.lg.Debug("onEstablishAck", zap.String("remote", conn.Remote()), zap.Int32("mode", resp.Mode))
	switch resp.Mode {
	case common.ModeWorker:
		cm.lg.Info("Worker connected ", zap.String("remote", conn.Remote()))
		cm.srv.workerConns[conn.Remote()] = conn
	case common.ModeGCS:
		cm.srv.gcsConns[conn.Remote()] = conn
	}
	conn.NotifyEstablish()
	return nil
}

func (cm *connectionManager) onAssignTaskResp(conn common.Connection, header *codec.Header, resp *pb.AssignTaskResponse) error {
	if resp.Accepted {
		cm.srv.pendingTasks.Delete(resp.TaskId)
		cm.lg.Info("Task accepted ", zap.String("task id", resp.TaskId))
	} else {
		cm.lg.Info("Task rejected ", zap.String("task id", resp.TaskId))
		task, ok := cm.srv.pendingTasks.Load(resp.TaskId)
		if !ok {
			return errors.New("task not found")
		}
		cm.srv.pendingTasks.Delete(resp.TaskId)
		cm.srv.PushTask(task.(common.Task))
	}
	//TODO
	return nil
}

func (cm *connectionManager) Ontick() {
	cm.conns.Range(func(key, value interface{}) bool {
		cm.lg.Debug("ontick", zap.Uint64("key", key.(uint64)))
		conn, ok := value.(*connection)
		if !ok {
			return true
		}
		conn.Ontick()
		return true
	})
}

// 主动建联的包处理
func (cm *connectionManager) HandlePacket(conn common.Connection, header *codec.Header, payload proto.Message) error {
	return cm.OnPacket(conn, header, payload)
}

func (cm *connectionManager) GetMode() int32 {
	return common.ModeScheduler
}
