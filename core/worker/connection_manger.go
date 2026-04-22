package worker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"mini-ray/codec"
	"mini-ray/common"
	pb "mini-ray/proto"

	"github.com/google/uuid"
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

	conns     sync.Map // uint64 -> *connection
	connAddrs sync.Map // string -> common.Connection

	gcsConn            common.Connection
	registerObjectChan chan ObjectID

	getObjectLocationChan map[string]chan string
	getObjectChan         map[string]chan []byte
	options               common.Options
}

func NewConnectionManager(ctx context.Context, lg *zap.Logger, advertiseAddr string, srv *worker, opts ...common.Option) *connectionManager {
	options := common.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &connectionManager{
		ctx:                ctx,
		lg:                 lg,
		advertiseAddr:      advertiseAddr,
		srv:                srv,
		pool:               srv.pool,
		registerObjectChan: make(chan ObjectID, options.RegisterObjectChanSize),
		options:            options,
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
	cm.connAddrs.Store(remote, conn)
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
	case codec.RayGetObjectData:
		cm.lg.Debug("onGetObjectData", zap.String("remote", conn.Remote()))
		req, _ := payload.(*pb.GetObjectDataRequest)
		return cm.onGetObjectData(conn, req)
	case codec.RayGetObjectLocationResp:
		cm.lg.Debug("onGetObjectLocationResp", zap.String("remote", conn.Remote()))
		resp, _ := payload.(*pb.GetObjectLocationResponse)
		return cm.onGetObjectLocationResp(resp)
	case codec.RayGetObjectDataResp:
		cm.lg.Debug("onGetObjectDataResp", zap.String("remote", conn.Remote()))
		resp, _ := payload.(*pb.GetObjectDataResponse)
		return cm.onGetObjectDataResp(resp)
	}
	return nil
}

func (cm *connectionManager) onEstablish(conn common.Connection, header *codec.Header, req *pb.EstablishReq) error {
	conn.SetEstablished()
	cm.lg.Debug("onEstablish", zap.String("remote", conn.Remote()), zap.String("mode", string(req.Mode)))
	if req.Mode == common.ModeGCS {
		cm.gcsConn = conn
	}
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
		ArgsType:     req.ArgsType,
	}
	for i, arg := range task.Args {
		argType := task.ArgsType[i]
		if argType == string(common.ObjectTypeObject) {
			objectID, err := DecodeString(arg)
			if err != nil {
				return err
			}
			object := cm.srv.objectStorage.GetObject(ObjectID(objectID.(string)))
			if object != nil {
				task.Args[i] = object.Data
			} else {
				objectData, err := cm.getRemoteObject(objectID.(ObjectID))
				if err != nil {
					return err
				}
				task.Args[i] = objectData
			}
		}

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

func (cm *connectionManager) onGetObjectData(conn common.Connection, req *pb.GetObjectDataRequest) error {
	objectID := ObjectID(req.ObjectId)
	object := cm.srv.objectStorage.GetObject(objectID)
	if object == nil {
		header := &codec.Header{Uri: codec.RayGetObjectDataResp, SeqId: atomic.AddUint64(&cm.nextID, 1)}
		resp := &pb.GetObjectDataResponse{Found: false, Error: "object not found"}
		payload, err := codec.EncodePayload(header, resp)
		if err != nil {
			return err
		}
		conn.Send(payload)
		return nil
	}
	header := &codec.Header{Uri: codec.RayGetObjectDataResp, SeqId: atomic.AddUint64(&cm.nextID, 1)}
	resp := &pb.GetObjectDataResponse{Found: true, Data: object.Data}
	payload, err := codec.EncodePayload(header, resp)
	if err != nil {
		return err
	}
	conn.Send(payload)
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
func (cm *connectionManager) Loop() {
	for {
		select {
		case objectID := <-cm.registerObjectChan:
			cm.lg.Debug("registerObject", zap.String("objectID", string(objectID)))
			header := &codec.Header{Uri: codec.RayReportTaskResult, SeqId: atomic.AddUint64(&cm.nextID, 1)}
			payload, err := codec.EncodePayload(header, &pb.ReportTaskResultRequest{ObjectId: string(objectID)})
			if err != nil {
				cm.lg.Error("encode payload", zap.Error(err))
				continue
			}
			cm.gcsConn.Send(payload)
		case <-cm.ctx.Done():
			return
		}
	}
}

// 主动建联的包处理
func (cm *connectionManager) HandlePacket(conn common.Connection, header *codec.Header, payload proto.Message) error {
	return nil
}

func (cm *connectionManager) GetMode() int32 {
	return common.ModeWorker
}

func (cm *connectionManager) getRemoteObject(objectID ObjectID) ([]byte, error) {
	if cm.gcsConn == nil {
		return nil, fmt.Errorf("gcs connection not found")
	}
	getObjectLocationUUID := uuid.New().String()
	header := &codec.Header{Uri: codec.RayGetObjectLocation, SeqId: atomic.AddUint64(&cm.nextID, 1)}
	payload, err := codec.EncodePayload(header, &pb.GetObjectLocationRequest{ObjectId: string(objectID), Uuid: getObjectLocationUUID})
	if err != nil {
		return nil, err
	}
	cm.gcsConn.Send(payload)

	select {
	case objectLocation := <-cm.getObjectLocationChan[getObjectLocationUUID]:
		getObjectUUID := uuid.New().String()
		header := &codec.Header{Uri: codec.RayGetObjectData, SeqId: atomic.AddUint64(&cm.nextID, 1)}
		payload, err := codec.EncodePayload(header, &pb.GetObjectDataRequest{ObjectId: string(objectID), Uuid: getObjectUUID})
		if err != nil {
			return nil, err
		}
		cm.getConnection(objectLocation).Send(payload)
		select {
		case objectData := <-cm.getObjectChan[getObjectUUID]:
			return objectData, nil
		case <-cm.ctx.Done():
			return nil, fmt.Errorf("context done")
		}
	case <-cm.ctx.Done():
		return nil, fmt.Errorf("context done")
	}
}

func (cm *connectionManager) onGetObjectLocationResp(resp *pb.GetObjectLocationResponse) error {
	cm.getObjectLocationChan[resp.Uuid] <- resp.NodeAddr
	return nil
}

func (cm *connectionManager) onGetObjectDataResp(resp *pb.GetObjectDataResponse) error {
	cm.getObjectChan[resp.Uuid] <- resp.Data
	return nil
}

func (cm *connectionManager) getConnection(address string) common.Connection {
	conn, ok := cm.conns.Load(address)
	if !ok {
		return nil
	}
	return conn.(common.Connection)
}
