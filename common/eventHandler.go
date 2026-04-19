package common

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"mini-ray/codec"

	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
)

var (
	gcli *gnet.Client
	eh   = &eventHandler{}
)

type eventHandler struct {
	gnet.BuiltinEventEngine
	eng gnet.Engine
}

func (eh *eventHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	eh.eng = eng
	return gnet.None
}

func (eh *eventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	if conn, ok := c.Context().(GnetContextConn); ok {
		conn.SetLocal(c.LocalAddr().String())
	}
	return nil, gnet.None
}

func (eh *eventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if conn, ok := c.Context().(GnetContextConn); ok {
		conn.OnClose(c)
	}
	return gnet.None
}

// OnTraffic 在 gnet 事件循环线程执行。
func (eh *eventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	ctx := c.Context()
	if ctx == nil {
		return gnet.Close
	}
	conn, ok := ctx.(GnetContextConn)
	if !ok {
		return gnet.Close
	}
	lg := conn.Logger()

	for c.InboundBuffered() > 0 {
		h, msg, err := codec.ZeroCopyDecode(c)
		if err == io.ErrShortBuffer {
			return gnet.None
		}
		if err != nil {
			if lg != nil {
				lg.Error("decode", zap.Error(err))
			}
			return gnet.Close
		}
		if err = conn.OnPacket(h, msg); err != nil {
			if lg != nil {
				lg.Error("OnPacket", zap.Error(err))
			}
			return gnet.Close
		}
	}
	return gnet.None
}

func init() {
	var err error
	gcli, err = gnet.NewClient(eh, gnet.WithTCPKeepAlive(DefaultShakehandTimeout*time.Second))
	if err != nil {
		panic(fmt.Sprintf("common: gnet.NewClient: %v", err))
	}
	if err := gcli.Start(); err != nil {
		panic(fmt.Sprintf("common: gnet client start: %v", err))
	}
	RegisterHandler(eh)
}

// DialWithTimeout 将 TCP 连接 enroll 到全局 gnet Client；ctx 须实现 GnetContextConn（如 *ClientConn）。
func DialWithTimeout(endpoint string, timeout time.Duration, ctx any) (gnet.Conn, error) {
	conn, err := net.DialTimeout("tcp", endpoint, timeout)
	if err != nil {
		return nil, err
	}
	return gcli.EnrollContext(conn, ctx)
}

func (eh *eventHandler) SafeExit(s os.Signal) {
	_ = eh.eng.Stop(context.Background())
	_ = gcli.Stop()
}
