package driver

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"mini-ray/codec"
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
	srv           *Scheduler
	pool          *ants.Pool
	nextID        uint64
	nextSeq       uint64

	conns sync.Map // uint64 -> *connection
}

func NewConnectionManager(lg *zap.Logger, advertiseAddr string, srv *Scheduler) *connectionManager {
	return &connectionManager{
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
	}
	return nil
}

func (cm *connectionManager) onEstablish(conn *connection, header *codec.Header, req *pb.EstablishReq) error {
	conn.established.SetTo(true)
	resp := &pb.EstablishResponse{Err: "OK"}
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
