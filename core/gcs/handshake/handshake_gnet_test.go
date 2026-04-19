package handshake

import (
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// This test demonstrates how to "listen gnet" by wiring gnet.Run() to an
// EventHandler that can decode/encode a protobuf handshake over TCP.
//
// It doesn't use the project's incomplete mini-ray gcs internals; instead it
// shows the minimal building blocks you need for your gcs.connMgr.

type hsServer struct {
	gnet.BuiltinEventEngine

	connected int32
	got       chan string
}

func (s *hsServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddInt32(&s.connected, 1)
	return nil, gnet.None
}

func (s *hsServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	// Packet format: [u32_be length][protobuf bytes]
	// For this test we assume exactly one handshake frame per connection.
	if c.InboundBuffered() < 4 {
		return gnet.None
	}

	hdr, _ := c.Peek(4)
	if len(hdr) < 4 {
		return gnet.None
	}
	n := binary.BigEndian.Uint32(hdr)
	if c.InboundBuffered() < 4+int(n) {
		return gnet.None
	}

	_, _ = c.Next(4) // consume header
	payload, _ := c.Next(int(n))

	var msg wrapperspb.StringValue
	if err := proto.Unmarshal(payload, &msg); err != nil {
		// Close the connection if the frame can't be decoded.
		return gnet.Close
	}

	select {
	case s.got <- msg.Value:
	default:
	}

	ackMsg := &wrapperspb.StringValue{Value: "ack-" + msg.Value}
	ackPayload, err := proto.Marshal(ackMsg)
	if err != nil {
		return gnet.Close
	}
	ackFrame := make([]byte, 4+len(ackPayload))
	binary.BigEndian.PutUint32(ackFrame[:4], uint32(len(ackPayload)))
	copy(ackFrame[4:], ackPayload)

	_, _ = c.Write(ackFrame)
	return gnet.Close
}

func (s *hsServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddInt32(&s.connected, -1)
	if atomic.LoadInt32(&s.connected) == 0 {
		return gnet.Shutdown
	}
	return gnet.None
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate tcp port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func dialHandshake(t *testing.T, addr string, value string, timeout time.Duration) (string, error) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}

		_ = conn.SetDeadline(time.Now().Add(1 * time.Second))

		reqMsg := &wrapperspb.StringValue{Value: value}
		reqPayload, err := proto.Marshal(reqMsg)
		if err != nil {
			_ = conn.Close()
			return "", err
		}
		frame := make([]byte, 4+len(reqPayload))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(reqPayload)))
		copy(frame[4:], reqPayload)

		_, err = conn.Write(frame)
		if err != nil {
			lastErr = err
			_ = conn.Close()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			lastErr = err
			_ = conn.Close()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		respPayload := make([]byte, n)
		if _, err := io.ReadFull(conn, respPayload); err != nil {
			lastErr = err
			_ = conn.Close()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		var resp wrapperspb.StringValue
		if err := proto.Unmarshal(respPayload, &resp); err != nil {
			_ = conn.Close()
			return "", err
		}

		_ = conn.Close()
		return resp.Value, nil
	}

	return "", lastErr
}

func TestTwoGCSHandshake(t *testing.T) {
	addr1 := freeTCPAddr(t)
	addr2 := freeTCPAddr(t)

	got1 := make(chan string, 1)
	got2 := make(chan string, 1)

	s1 := &hsServer{got: got1}
	s2 := &hsServer{got: got2}

	done1 := make(chan struct{})
	done2 := make(chan struct{})

	go func() {
		// gnet.Run binds and blocks until the handler triggers gnet.Shutdown.
		_ = gnet.Run(s1, "tcp://"+addr1, gnet.WithMulticore(false))
		close(done1)
	}()
	go func() {
		_ = gnet.Run(s2, "tcp://"+addr2, gnet.WithMulticore(false))
		close(done2)
	}()

	ackCh := make(chan string, 2)
	errCh := make(chan error, 2)

	go func() {
		resp, err := dialHandshake(t, addr2, "from-1", 3*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		ackCh <- resp
	}()
	go func() {
		resp, err := dialHandshake(t, addr1, "from-2", 3*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		ackCh <- resp
	}()

	var ack1, ack2 string
	select {
	case err := <-errCh:
		t.Fatalf("dial failed: %v", err)
	case ack1 = <-ackCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for ack1")
	}
	select {
	case err := <-errCh:
		t.Fatalf("dial failed: %v", err)
	case ack2 = <-ackCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for ack2")
	}

	if ack1 != "ack-from-2" && ack1 != "ack-from-1" {
		t.Fatalf("unexpected ack1: %q", ack1)
	}
	if ack2 != "ack-from-2" && ack2 != "ack-from-1" {
		t.Fatalf("unexpected ack2: %q", ack2)
	}

	// Ensure both servers observed the incoming handshake.
	select {
	case v := <-got1:
		if v != "from-2" {
			t.Fatalf("server1 got %q, want %q", v, "from-2")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting server1 handshake")
	}
	select {
	case v := <-got2:
		if v != "from-1" {
			t.Fatalf("server2 got %q, want %q", v, "from-1")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting server2 handshake")
	}

	// Wait for both gnet engines to exit.
	select {
	case <-done1:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting server1 exit")
	}
	select {
	case <-done2:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting server2 exit")
	}
}

