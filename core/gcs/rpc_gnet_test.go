package gcs

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"mini-ray/codec"
	"mini-ray/common"
	pb "mini-ray/proto"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

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

func waitReachable(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server not reachable: %s", addr)
}

func readOneOuterFrame(conn net.Conn) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > common.MaxFrameSize {
		return nil, io.ErrUnexpectedEOF
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func TestGnetCodec_Establish(t *testing.T) {
	addr := freeTCPAddr(t)

	pool, err := ants.NewPool(8)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Release()

	srv := NewGCS(context.Background(), zap.NewNop(), pool, addr, "test-adv")
	srv.Start()

	waitReachable(t, addr)

	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	inner, err := codec.EncodePayload(&codec.Header{Uri: codec.MetaEstablish, SeqId: 1}, &pb.EstablishReq{Addr: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(common.MakeFrame(inner)); err != nil {
		t.Fatal(err)
	}

	payload, err := readOneOuterFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	_, msg, err := codec.DecodeInnerPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	resp, ok := msg.(*pb.EstablishResponse)
	if !ok {
		t.Fatalf("expected EstablishResponse, got %T", msg)
	}
	if resp.Err == "" {
		t.Fatal("empty Err")
	}
}

// A dials B, starts callback read loop, sends Establish; B answers via gnet OnPacket. Inspect logs (e.g. on uri / dial on uri / onEstablish).
func TestDialEstablish_Trigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lg, _ := zap.NewDevelopment()

	pool, err := ants.NewPool(8)
	if err != nil {
		t.Fatalf("ants pool: %v", err)
	}
	defer pool.Release()

	addrA := freeTCPAddr(t)
	addrB := freeTCPAddr(t)

	gcsA := NewGCS(ctx, lg, pool, addrA, addrA)
	gcsB := NewGCS(ctx, lg, pool, addrB, addrB)

	gcsA.Start()
	gcsB.Start()
	time.Sleep(1 * time.Second)
	// waitReachable(t, addrA)
	// waitReachable(t, addrB)

	gcsA.Connect(ctx, addrB)
	time.Sleep(1 * time.Second)
	go gcsB.Loop()

	time.Sleep(3 * time.Second)
}
