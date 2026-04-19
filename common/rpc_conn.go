package common

import (
	"encoding/binary"
	"mini-ray/codec"

	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Connection 最小发送/关闭能力（入站 Conn 与主动 ClientConn 均可实现）。
type Connection interface {
	ID() uint64
	Remote() string
	Send(payload []byte) error
	Close() error
	SetEstablished()
	NotifyEstablish()
	OnClose(conn gnet.Conn)
	Logger() *zap.Logger
	OnPacket(header *codec.Header, msg proto.Message) error
	Start()
	Stop()
	Loop()
}

// ReadHandler 解码后的帧分发（在 gnet 事件线程或读循环中调用）。
type ReadHandler interface {
	OnPacket(header *codec.Header, msg proto.Message) error
}

// GnetContextConn 通过 DialWithTimeout 注册到全局 gnet Client 时的 Context；
// eventHandler 在 OnOpen/OnClose/OnTraffic 中依赖此接口。
type GnetContextConn interface {
	Connection
	ReadHandler
	SetLocal(local string)
	Logger() *zap.Logger
	OnClose(conn gnet.Conn)
}

const MaxFrameSize = 16 << 20

func MakeFrame(payload []byte) []byte {
	b := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(b[:4], uint32(len(payload)))
	copy(b[4:], payload)
	return b
}

func ReadFrameFromGnet(c gnet.Conn) (payload []byte, ok bool) {
	if c.InboundBuffered() < 4 {
		return nil, false
	}
	hdr, _ := c.Peek(4)
	if len(hdr) < 4 {
		return nil, false
	}
	n := binary.BigEndian.Uint32(hdr)
	if c.InboundBuffered() < 4+int(n) {
		return nil, false
	}
	_, _ = c.Next(4)
	p, _ := c.Next(int(n))
	return p, true
}
