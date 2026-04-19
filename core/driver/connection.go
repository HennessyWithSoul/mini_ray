package driver

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"mini-ray/codec"
	"mini-ray/common"
	pb "mini-ray/proto"

	"github.com/panjf2000/gnet/v2"
	"github.com/tevino/abool/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var _ common.ReadHandler = (*connection)(nil)

type connection struct {
	id          uint64
	remote      string
	cm          *connectionManager
	lg          *zap.Logger
	gnetC       gnet.Conn
	netC        net.Conn
	isOutbound  bool
	established abool.AtomicBool

	establishChan chan struct{}
}

func newConnection(cm *connectionManager, id uint64, remote string, gc gnet.Conn, lg *zap.Logger) *connection {
	return &connection{
		cm:            cm,
		id:            id,
		remote:        remote,
		gnetC:         gc,
		lg:            lg,
		establishChan: make(chan struct{}, 1),
	}
}

func (c *connection) ID() uint64     { return c.id }
func (c *connection) Remote() string { return c.remote }

func (c *connection) Send(payload []byte) error {
	frame := common.MakeFrame(payload)
	if c.netC != nil {
		return writeFull(c.netC, frame)
	}
	if c.gnetC != nil {
		return c.gnetC.AsyncWrite(frame, func(_ gnet.Conn, err error) error { return err })
	}
	return fmt.Errorf("connection: no transport")
}

func (c *connection) OnPacket(header *codec.Header, msg proto.Message) error {
	err := c.cm.OnPacket(c, header, msg)
	if err != nil {
		return err
	}
	return nil
}

func (c *connection) readLoop(ctx context.Context) {
	if c.netC == nil {
		return
	}
	go func() {
		<-ctx.Done()
		_ = c.Close()
	}()
	for {
		raw, err := c.readFrame()
		if err != nil {
			return
		}
		hdr, msg, err := codec.DecodeInnerPayload(raw)
		if err != nil {
			continue
		}
		if err := c.cm.OnPacket(c, hdr, msg); err != nil {
			_ = c.Close()
			return
		}
	}
}

func (c *connection) readFrame() ([]byte, error) {
	if c.netC == nil {
		return nil, fmt.Errorf("no net.Conn")
	}
	var hdr [4]byte
	if _, err := io.ReadFull(c.netC, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > common.MaxFrameSize {
		return nil, fmt.Errorf("frame too large")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.netC, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *connection) Close() error {
	if c.gnetC != nil {
		return c.gnetC.Close()
	}
	if c.netC != nil {
		return c.netC.Close()
	}
	return nil
}

func writeFull(w net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func (c *connection) Start() {
	c.cm.pool.Submit(func() {
		c.Loop()
	})
}

func (c *connection) Loop() {
	for {
		dialer := net.Dialer{Timeout: 5 * time.Second}
		nc, err := dialer.DialContext(context.Background(), "tcp", c.remote)
		if err != nil {
			c.lg.Error("dial", zap.Error(err))
			continue
		}
		c.netC = nc
		c.cm.pool.Submit(func() {
			c.readLoop(context.Background())
		})
		c.Establish()
		select {
		case <-time.After(5 * time.Second):
			continue
		case <-c.establishChan:
			c.established.SetToIf(false, true)
			c.cm.conns.Store(c.id, c)
			c.lg.Info("established", zap.String("remote", c.remote))
		}
		for c.established.IsSet() {
			//do shakehand
		}
	}
}

func (c *connection) Establish() {
	seq := atomic.AddUint64(&c.cm.nextSeq, 1)
	payload, err := codec.EncodePayload(
		&codec.Header{Uri: codec.MetaEstablish, SeqId: seq},
		&pb.EstablishReq{Addr: c.cm.advertiseAddr, Type: c.cm.nodeType},
	)
	if err != nil {
		c.lg.Error("encode establish", zap.Error(err))
		return
	}
	if err := c.Send(payload); err != nil {
		c.lg.Error("send establish", zap.Error(err))
		return
	}
}
func (c *connection) ShakeHand() {
	seq := atomic.AddUint64(&c.cm.nextSeq, 1)
	payload, err := codec.EncodePayload(
		&codec.Header{Uri: codec.MetaShakeHand, SeqId: seq},
		&pb.ShakeHandReq{},
	)
	if err != nil {
		c.lg.Error("encode shake hand", zap.Error(err))
		return
	}
	if err := c.Send(payload); err != nil {
		c.lg.Error("send shake hand", zap.Error(err))
		return
	}
}

func (c *connection) NotifyEstablish() {
	select {
	case c.establishChan <- struct{}{}:
	default:
	}
}

func (c *connection) Ontick() {
	if c.established.IsNotSet() {
		return
	}
	c.lg.Debug("shake hand", zap.String("remote", c.remote))
	c.ShakeHand()
}
