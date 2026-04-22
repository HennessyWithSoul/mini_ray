package gcs

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
	srv           *GCS
	pool          *ants.Pool
	nextID        uint64
	nextSeq       uint64

	conns sync.Map // uint64 -> *connection
}

func NewConnectionManager(ctx context.Context, lg *zap.Logger, advertiseAddr string, srv *GCS) *connectionManager {
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
	case codec.RayReportTaskResult:
		req, _ := payload.(*pb.ReportTaskResultRequest)
		cm.lg.Debug("onReportTaskResult", zap.String("remote", conn.Remote()), zap.String("onjectID", req.ObjectId))
		return cm.onReportTaskResult(conn, req)
	case codec.RayGetObjectLocation:
		req, _ := payload.(*pb.GetObjectDataRequest)
		cm.lg.Debug("onGetObjectData", zap.String("remote", conn.Remote()), zap.String("objectID", req.ObjectId))
		return cm.onGetObjectLocation(conn, req)
	}
	return nil
}

func (cm *connectionManager) onEstablish(conn common.Connection, header *codec.Header, req *pb.EstablishReq) error {
	conn.SetEstablished()
	resp := &pb.EstablishResponse{Err: "OK"}
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

func (cm *connectionManager) onReportTaskResult(conn common.Connection, req *pb.ReportTaskResultRequest) error {
	cm.srv.objectLocationMap[ObjectID(req.ObjectId)] = conn.Remote()
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

func (cm *connectionManager) onGetObjectLocation(conn common.Connection, req *pb.GetObjectDataRequest) error {
	objectID := ObjectID(req.ObjectId)
	location, ok := cm.srv.objectLocationMap[objectID]
	if !ok {
		header := &codec.Header{Uri: codec.RayGetObjectLocationResp, SeqId: atomic.AddUint64(&cm.nextID, 1)}
		resp := &pb.GetObjectLocationResponse{NodeAddr: ""}
		payload, err := codec.EncodePayload(header, resp)
		if err != nil {
			return err
		}
		return conn.Send(payload)
	}
	header := &codec.Header{Uri: codec.RayGetObjectLocationResp, SeqId: atomic.AddUint64(&cm.nextID, 1)}
	resp := &pb.GetObjectLocationResponse{NodeAddr: location}
	payload, err := codec.EncodePayload(header, resp)
	if err != nil {
		return err
	}
	return conn.Send(payload)
}

// 主动建联的包处理
func (cm *connectionManager) HandlePacket(conn common.Connection, header *codec.Header, payload proto.Message) error {
	return cm.OnPacket(conn, header, payload)
}

func (cm *connectionManager) GetMode() int32 {
	return common.ModeGCS
}
