package driver

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"mini-ray/codec"
	common "mini-ray/common"
	pb "mini-ray/proto"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type connectionManager struct {
	gnet.BuiltinEventEngine

	lg            *zap.Logger
	advertiseAddr string
	srv           *Driver
	pool          *ants.Pool
	nextID        uint64
	nextSeq       uint64
	nodeMode      int

	conns     sync.Map // uint64 -> *connection
	typeConns sync.Map // type -> []*connection
}

func NewConnectionManager(lg *zap.Logger, advertiseAddr string, srv *Driver) *connectionManager {
	return &connectionManager{
		lg:            lg,
		advertiseAddr: advertiseAddr,
		srv:           srv,
		pool:          srv.pool,
		nodeMode:      common.ModeDriver,
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
	conn := newConnection(cm, id, remote, c, cm.lg)
	c.SetContext(conn)
	cm.conns.Store(id, conn)
	cm.lg.Debug("gnet OnOpen", zap.Uint64("id", id), zap.String("remote", remote))
	return nil, gnet.None
}

func (cm *connectionManager) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if v := c.Context(); v != nil {
		if conn, ok := v.(*connection); ok {
			cm.conns.Delete(conn.ID())
		}
	}
	return gnet.None
}

func (cm *connectionManager) OnTraffic(c gnet.Conn) (action gnet.Action) {
	if cm.srv != nil && cm.srv.started.IsNotSet() {
		return gnet.Shutdown
	}

	conn, ok := c.Context().(*connection)
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
	conn := newConnection(cm, id, addr, nil, cm.lg)
	conn.Start()
}

func (cm *connectionManager) OnPacket(conn *connection, header *codec.Header, payload proto.Message) error {
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
	case codec.SchedulerSubmitTaskResp:
		req, _ := payload.(*pb.SchedulerSubmitTaskResponse)
		cm.lg.Debug("onSchedulerSubmitTaskResp", zap.String("remote", conn.Remote()), zap.String("task", req.String()))
		cm.srv.onSchedulerSubmitTaskResp(req)
	}
	return nil
}

func (cm *connectionManager) onEstablish(conn *connection, header *codec.Header, req *pb.EstablishReq) error {
	conn.established.SetTo(true)
	typeConns, _ := cm.typeConns.LoadOrStore(req.Mode, &sync.Map{})
	typeConns.(*sync.Map).Store(conn.ID(), conn)
	resp := &pb.EstablishResponse{Mode: int32(cm.nodeMode), Err: common.EstablishResponseOK}
	ack := &codec.Header{Uri: codec.MetaEstablishAck, SeqId: header.SeqId}
	payload, err := codec.EncodePayload(ack, resp)
	if err != nil {
		return err
	}
	return conn.Send(payload)
}

func (cm *connectionManager) onEstablishAck(conn *connection) error {
	conn.NotifyEstablish()
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

func (cm *connectionManager) SubmitTask(ctx context.Context, task *pb.DriverSubmitTaskRequest) {
	cm.lg.Debug("submitTask", zap.String("task", task.String()))
	typeConns, ok := cm.typeConns.Load(common.NodeTypeScheduler)
	if !ok || common.GetSyncMapLen(typeConns.(*sync.Map)) == 0 {
		cm.lg.Error("no scheduler connection", zap.String("type", common.NodeTypeScheduler))
		return
	}
	connectionSlice := make([]*connection, 0)
	typeConns.(*sync.Map).Range(func(key, value interface{}) bool {
		connectionSlice = append(connectionSlice, value.(*connection))
		return true
	})
	for _, conn := range connectionSlice {
		header := &codec.Header{Uri: codec.DriverSubmitTask, SeqId: atomic.AddUint64(&cm.nextSeq, 1)}
		payload, err := codec.EncodePayload(header, task)
		if err != nil {
			cm.lg.Error("encode task", zap.Error(err))
			continue
		}
		cm.srv.taskNotifyChan[task.TaskId] = make(chan string)
		conn.Send(payload)
		select {
		case <-time.After(common.SubmitTaskTimeout):
			cm.lg.Error("submit task timeout", zap.String("task", task.String()))
			delete(cm.srv.taskNotifyChan, task.TaskId)
		case result := <-cm.srv.taskNotifyChan[task.TaskId]:
			delete(cm.srv.taskNotifyChan, task.TaskId)
			if result == common.SubmitTaskSuccess {
				return
			} else {
				continue
			}
		}
	}
}

func (cm *connectionManager) onSchedulerSubmitTaskResp(resp *pb.SchedulerSubmitTaskResponse) {
	cm.lg.Debug("onSchedulerSubmitTaskResp", zap.String("resp", resp.String()))
	if resp.Success {
		cm.lg.Debug("submit task success", zap.String("task", resp.TaskId))
	} else {
		cm.lg.Debug("submit task failed", zap.String("task", resp.TaskId))
	}
}
