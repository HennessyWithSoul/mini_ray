package worker

import (
	"context"
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
	srv           *worker
	pool          *ants.Pool
	nextID        uint64

	conns sync.Map // uint64 -> *connection
}

func NewConnectionManager(ctx context.Context, lg *zap.Logger, advertiseAddr string, srv *worker) *connectionManager {
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
	switch header.Uri {
	case codec.MetaEstablish:
		req, _ := payload.(*pb.EstablishReq)
		cm.lg.Debug("onEstablish", zap.String("remote", conn.Remote()), zap.String("addr", req.Addr))
		return cm.onEstablish(conn, header, req)
	case codec.MetaEstablishAck:
		cm.lg.Debug("onEstablishAck", zap.String("remote", conn.Remote()))
		return cm.onEstablishAck(conn)
	case codec.MetaShakeHand:
		cm.lg.Debug("onShakeHand", zap.String("remote", conn.Remote()))
	case codec.RayAssignTask:
		cm.lg.Debug("onAssignTask", zap.String("remote", conn.Remote()))
		req, _ := payload.(*pb.AssignTaskRequest)
		return cm.onAssignTask(conn, header, req)
	}
	return nil
}

func (cm *connectionManager) onEstablish(conn common.Connection, header *codec.Header, req *pb.EstablishReq) error {
	conn.SetEstablished()
	resp := &pb.EstablishResponse{Err: "OK", Mode: common.ModeWorker}
	ack := &codec.Header{Uri: codec.MetaEstablishAck, SeqId: header.SeqId}
	payload, err := codec.EncodePayload(ack, resp)
	if err != nil {
		return err
	}
	return conn.Send(payload)
}

func (cm *connectionManager) onEstablishAck(conn common.Connection) error {
	conn.NotifyEstablish()
	return nil
}

func (cm *connectionManager) onAssignTask(conn common.Connection, header *codec.Header, req *pb.AssignTaskRequest) error {
	funcName := req.FuncName
	task := common.Task{
		ID:           req.TaskId,
		FuncName:     funcName,
		Args:         req.Args,
		Dependencies: req.Dependencies,
	}
	var err error
	switch funcName {
	case common.TaskFuncAddTowInt:
		err = cm.srv.onAddTowInt(conn, task)
	}
	resp := &pb.AssignTaskResponse{Accepted: err == nil, TaskId: task.ID}
	ack := &codec.Header{Uri: codec.RayAssignTaskResp, SeqId: header.SeqId}
	payload, err := codec.EncodePayload(ack, resp)
	conn.Send(payload)
	return err
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
	return nil
}

func (cm *connectionManager) GetMode() int32 {
	return common.ModeWorker
}
