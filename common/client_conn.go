package common

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mini-ray/codec"
	pb "mini-ray/proto"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/tevino/abool/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	_ Connection  = (*connection)(nil)
	_ ReadHandler = (*connection)(nil)

	clientConnIDSeq uint64
)

// ClientPacketHandler Establish 完成后处理业务 URI（各节点可选实现）。
type ClientPacketHandler interface {
	HandlePacket(cc Connection, h *codec.Header, msg proto.Message) error
	GetMode() int32
}

// ClientConnOption 可选配置。
type ConnectionOption func(*connection)

func WithEstablishTimeout(d time.Duration) ConnectionOption {
	return func(c *connection) { c.establishTimeout = d }
}

func WithRecvIdleTimeout(d time.Duration) ConnectionOption {
	return func(c *connection) { c.recvIdleTimeout = d }
}

func WithShakeInterval(d time.Duration) ConnectionOption {
	return func(c *connection) { c.shakeEvery = d }
}

func WithDialTimeout(d time.Duration) ConnectionOption {
	return func(c *connection) { c.dialTimeout = d }
}

// ClientConn 主动建联：经全局 gnet Client + eventHandler，双层 context、退避重连、Establish + 心跳。
// 各节点（driver/worker/gcs/scheduler）共用此类型即可。
type connection struct {
	ctx    context.Context
	cancel context.CancelFunc

	subCtx           context.Context
	subCancel        context.CancelFunc
	localAddr        string
	id               uint64
	endpoint         string
	local            string
	seqID            uint64
	failure          int64
	nextDial         time.Time
	dialTimeout      time.Duration
	establishTimeout time.Duration
	recvIdleTimeout  time.Duration
	shakeEvery       time.Duration

	started     abool.AtomicBool
	established abool.AtomicBool

	lastRecv time.Time
	lastSent time.Time

	rawConn gnet.Conn
	pool    *ants.Pool
	lg      *zap.Logger

	handler ClientPacketHandler
}

// NewClientConn 创建主动连接；Start(pool) 后在后台 loop。Dial 时把自身作为 EnrollContext 交给 gnet。
func NewConnection(parent context.Context, lg *zap.Logger, pool *ants.Pool, id uint64, endpoint string, handler ClientPacketHandler, opts ...ConnectionOption) *connection {
	ctx, cancel := context.WithCancel(parent)
	c := &connection{
		ctx:              ctx,
		cancel:           cancel,
		pool:             pool,
		id:               id,
		endpoint:         endpoint,
		dialTimeout:      30 * time.Second,
		establishTimeout: 10 * time.Second,
		recvIdleTimeout:  30 * time.Second,
		shakeEvery:       3 * time.Second,
		lg:               lg,
		handler:          handler,
	}
	if c.lg != nil {
		c.lg = c.lg.With(zap.String("endpoint", endpoint), zap.Uint64("cid", c.id))
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *connection) ID() uint64     { return c.id }
func (c *connection) Remote() string { return c.endpoint }

func (c *connection) Logger() *zap.Logger { return c.lg }

func (c *connection) SetLocal(local string) { c.local = local }

func (c *connection) Established() bool { return c.established.IsSet() }

func (c *connection) OnClose(_ gnet.Conn) {
	c.notifyReconnect("remote close")
}

// Start 提交 loop 到 ants pool（与 crsync 一致）。
func (c *connection) Start() {
	if c.started.SetToIf(false, true) {
		c.pool.Submit(c.Loop)
	}
}

// Stop 停止外层循环。
func (c *connection) Stop() {
	if c.started.SetToIf(true, false) {
		if c.lg != nil {
			c.lg.Info("client conn stop")
		}
		c.cancel()
		c.notifyReconnect("stop")
		c.established.SetTo(false)
	}
}

// Send 发送一帧内层 payload（会自动 MakeFrame）；需已 Establish。
func (c *connection) Send(innerPayload []byte) error {
	if c.rawConn == nil {
		return errors.New("common: ClientConn no rawConn")
	}
	if c.established.IsNotSet() {
		return errors.New("common: ClientConn not established")
	}
	frame := MakeFrame(innerPayload)
	return c.rawConn.AsyncWrite(frame, func(_ gnet.Conn, err error) error { return err })
}

// EncodeAndSend 编码 Header+protobuf 后发送。
func (c *connection) EncodeAndSend(h *codec.Header, msg proto.Message) error {
	if h.SeqId == 0 {
		h.SeqId = atomic.AddUint64(&c.seqID, 1)
	}
	inner, err := codec.EncodePayload(h, msg)
	if err != nil {
		return err
	}
	if err := c.Send(inner); err != nil {
		return err
	}
	if c.lg != nil {
		c.lg.Debug("encode send", zap.Uint16("uri", h.Uri), zap.Uint64("seq", h.SeqId))
	}
	c.lastSent = time.Now()
	return nil
}

func (c *connection) Close() error {
	c.Stop()
	return nil
}

func (c *connection) notifyReconnect(reason string) {
	if c.lg != nil {
		c.lg.Warn("notify reconnect", zap.String("reason", reason))
	}
	if c.subCancel != nil {
		c.subCancel()
	}
}

func (c *connection) waitClose() {
	c.established.SetTo(false)
	if c.rawConn == nil {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	start := time.Now()
	_ = c.rawConn.CloseWithCallback(func(_ gnet.Conn, _ error) error {
		if c.lg != nil {
			c.lg.Info("rawConn closed", zap.Duration("since", time.Since(start)))
		}
		wg.Done()
		return nil
	})
	wg.Wait()
	c.rawConn = nil
}

func floorToSecond(d time.Duration) time.Duration {
	sec := int(d.Seconds())
	if sec == 0 {
		return d
	}
	return time.Duration(sec) * time.Second
}

func getBackOff(failure int64) time.Duration {
	switch {
	case failure < 10:
		return 0
	case failure < 20:
		return floorToSecond(time.Millisecond * 10 * time.Duration(failure))
	default:
		return floorToSecond(time.Millisecond * 50 * time.Duration(failure))
	}
}

func (c *connection) dialOnce() (err error) {
	defer func() {
		if err != nil {
			atomic.AddInt64(&c.failure, 1)
			c.nextDial = time.Now().Add(getBackOff(atomic.LoadInt64(&c.failure)))
			if c.lg != nil {
				c.lg.Error("dial failed", zap.Time("next", c.nextDial), zap.Error(err))
			}
		}
	}()
	var gc gnet.Conn
	gc, err = DialWithTimeout(c.endpoint, c.dialTimeout, c)
	if err != nil {
		return err
	}
	c.rawConn = gc
	c.lastRecv = time.Now()
	c.lastSent = time.Now()
	c.nextDial = time.Now().Add(time.Hour)
	return nil
}

func (c *connection) sendEstablish() error {
	c.established.SetTo(false)
	seq := atomic.AddUint64(&c.seqID, 1)
	inner, err := codec.EncodePayload(
		&codec.Header{Uri: codec.MetaEstablish, SeqId: seq},
		&pb.EstablishReq{Addr: c.localAddr, Mode: c.handler.GetMode()},
	)
	if err != nil {
		return err
	}
	frame := MakeFrame(inner)
	if err := c.rawConn.AsyncWrite(frame, func(_ gnet.Conn, err error) error { return err }); err != nil {
		return err
	}
	c.lastSent = time.Now()
	return nil
}

func (c *connection) sendShakeHand() error {
	seq := atomic.AddUint64(&c.seqID, 1)
	inner, err := codec.EncodePayload(
		&codec.Header{Uri: codec.MetaShakeHand, SeqId: seq},
		&pb.ShakeHandReq{},
	)
	if err != nil {
		return err
	}
	frame := MakeFrame(inner)
	if err := c.rawConn.AsyncWrite(frame, func(_ gnet.Conn, err error) error { return err }); err != nil {
		c.notifyReconnect("async write")
		return err
	}
	c.lastSent = time.Now()
	return nil
}

func (c *connection) onTick() {
	if c.established.IsNotSet() {
		if time.Since(c.lastSent) > c.establishTimeout {
			c.notifyReconnect("establish timeout")
		}
		return
	}
	if time.Since(c.lastRecv) > c.recvIdleTimeout {
		c.notifyReconnect("recv idle")
	}
}

func (c *connection) maybeShake() {
	if !c.established.IsSet() {
		return
	}
	if time.Since(c.lastSent) < c.shakeEvery {
		return
	}
	if err := c.sendShakeHand(); err != nil && c.lg != nil {
		c.lg.Error("shakehand", zap.Error(err))
	}
}

func (c *connection) Loop() {
	defer func() {
		c.Stop()
		if e := recover(); e != nil {
			if c.lg != nil {
				c.lg.Error("client conn panic", zap.Any("panic", e))
			}
			if c.pool != nil {
				_ = c.pool.Submit(c.Loop)
			}
		}
	}()

	if c.lg != nil {
		c.lg.Info("client conn loop start")
	}
	wait := time.NewTimer(time.Hour)
	for c.started.IsSet() {
		select {
		case <-c.ctx.Done():
			c.started.SetTo(false)
			return
		default:
		}

		if err := c.dialOnce(); err != nil {
			wait.Reset(floorToSecond(time.Until(c.nextDial)))
			select {
			case <-wait.C:
				continue
			case <-c.ctx.Done():
				c.started.SetTo(false)
				return
			}
		}

		c.subCtx, c.subCancel = context.WithCancel(c.ctx)
		if err := c.sendEstablish(); err != nil {
			if c.lg != nil {
				c.lg.Error("establish send", zap.Error(err))
			}
			_ = c.rawConn.Close()
			c.rawConn = nil
			continue
		}

		tick := time.NewTicker(time.Second)
	inner:
		for {
			select {
			case <-c.subCtx.Done():
				break inner
			case <-tick.C:
				c.onTick()
				c.maybeShake()
			}
		}
		tick.Stop()
		c.waitClose()
	}
	if c.lg != nil {
		c.lg.Info("client conn loop exit")
	}
}

// OnPacket 在 gnet 事件线程调用（eventHandler → ReadHandler）。
func (c *connection) OnPacket(h *codec.Header, msg proto.Message) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("OnPacket panic: %v", e)
		}
	}()

	c.lastRecv = time.Now()
	c.nextDial = time.Now().Add(time.Hour)

	if c.established.IsNotSet() && h.Uri != codec.MetaEstablishAck {
		return nil
	}

	switch h.Uri {
	case codec.MetaEstablishAck:
		resp, _ := msg.(*pb.EstablishResponse)
		if resp != nil && resp.Err != "" && resp.Err != EstablishResponseOK {
			c.notifyReconnect("establish rejected")
			return fmt.Errorf("establish: %s", resp.Err)
		}
		c.established.SetTo(true)
		atomic.StoreInt64(&c.failure, 0)
		c.lg.Info("established", zap.String("local", c.local))
		if c.handler != nil {
			return c.handler.HandlePacket(c, h, msg)
		}
	case codec.MetaShakeHand, codec.MetaShakeHandResp:
		// 保活 / 对端心跳，刷新 lastRecv 即可
	default:
		c.lg.Debug("onPacket", zap.Uint16("uri", h.Uri))
		if c.handler != nil {
			return c.handler.HandlePacket(c, h, msg)
		}
	}
	return nil
}

func (c *connection) NotifyEstablish() {
	c.established.SetTo(true)
}

func (c *connection) SetEstablished() {
	c.established.SetTo(true)
}
