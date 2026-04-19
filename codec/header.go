package codec

import (
	"encoding/binary"
	"fmt"
	"io"

	pb "mini-ray/proto"

	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
)

// URI constants for RPC routing (extend as your .proto grows).
const (
	MetaEstablish     uint16 = 1
	MetaEstablishAck  uint16 = 2
	MetaShakeHand     uint16 = 3 // 请求：ShakeHandReq
	MetaShakeHandResp uint16 = 4 // 响应：ShakeHandResp（与 Establish / EstablishAck 成对）
)

// Header is the fixed prefix before each protobuf body (same idea as your original codec.Header).
type Header struct {
	Uri   uint16
	SeqId uint64
}

// EncodePayload builds [Uri:2][SeqId:8][proto body...] (inner payload before outer length frame).
func EncodePayload(h *Header, msg proto.Message) ([]byte, error) {
	body, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 10+len(body))
	binary.BigEndian.PutUint16(buf[0:2], h.Uri)
	binary.BigEndian.PutUint64(buf[2:10], h.SeqId)
	copy(buf[10:], body)
	return buf, nil
}

// ZeroCopyDecode reads one outer frame from gnet: [u32_be len][inner], inner = Header + protobuf body.
// Returns io.ErrShortBuffer if the TCP buffer does not yet contain a full frame.
func ZeroCopyDecode(c gnet.Conn) (*Header, proto.Message, error) {
	if c.InboundBuffered() < 4 {
		return nil, nil, io.ErrShortBuffer
	}
	lb, _ := c.Peek(4)
	if len(lb) < 4 {
		return nil, nil, io.ErrShortBuffer
	}
	outerLen := int(binary.BigEndian.Uint32(lb))
	if outerLen < 10 {
		return nil, nil, io.ErrShortBuffer
	}
	if c.InboundBuffered() < 4+outerLen {
		return nil, nil, io.ErrShortBuffer
	}
	_, _ = c.Next(4)
	payload, _ := c.Next(outerLen)
	if len(payload) < 10 {
		return nil, nil, io.ErrShortBuffer
	}
	h := &Header{
		Uri:   binary.BigEndian.Uint16(payload[0:2]),
		SeqId: binary.BigEndian.Uint64(payload[2:10]),
	}
	body := payload[10:]
	msg, err := unmarshalByURI(h.Uri, body)
	if err != nil {
		return nil, nil, err
	}
	return h, msg, nil
}

// DecodeInnerPayload parses inner bytes [Uri:2][SeqId:8][body] (after outer length frame is stripped).
func DecodeInnerPayload(payload []byte) (*Header, proto.Message, error) {
	if len(payload) < 10 {
		return nil, nil, io.ErrShortBuffer
	}
	h := &Header{
		Uri:   binary.BigEndian.Uint16(payload[0:2]),
		SeqId: binary.BigEndian.Uint64(payload[2:10]),
	}
	body := payload[10:]
	msg, err := unmarshalByURI(h.Uri, body)
	return h, msg, err
}

func unmarshalByURI(uri uint16, body []byte) (proto.Message, error) {
	if uri >= RayURIBase {
		return UnmarshalRayBody(uri, body)
	}
	switch uri {
	case MetaEstablish:
		m := new(pb.EstablishReq)
		return m, proto.Unmarshal(body, m)
	case MetaEstablishAck:
		m := new(pb.EstablishResponse)
		return m, proto.Unmarshal(body, m)
	case MetaShakeHand:
		m := new(pb.ShakeHandReq)
		return m, proto.Unmarshal(body, m)
	case MetaShakeHandResp:
		m := new(pb.ShakeHandResp)
		return m, proto.Unmarshal(body, m)
	default:
		return nil, fmt.Errorf("codec: unknown uri %d", uri)
	}
}

/*
cd /Users/hennessy/Documents/WorkSpace/mini-Ray &&
protoc -I proto --go_out=proto --go_opt=paths=source_relative --go-grpc_out=proto --go-grpc_opt=paths=source_relative proto/meta.proto 2>&1
*/

const (
	RayURIBase uint16 = 100

	DriverSubmitTask         = 100
	SchedulerSubmitTaskResp  = 101
	RayRegisterTaskToGCS     = 102
	RayRegisterTaskToGCSResp = 103
	RayFetchTask             = 104
	RayFetchTaskResp         = 105
	RayGetObjectLocation     = 106
	RayGetObjectLocationResp = 107
	RayReportTaskResult      = 108
	RayReportTaskResultResp  = 109
	RayGetTaskResult         = 110
	RayGetTaskResultResp     = 111
)

// UnmarshalRayBody 解码 ray.proto 中各请求/响应（与 Ray* 常量一一对应）。
func UnmarshalRayBody(uri uint16, body []byte) (proto.Message, error) {
	switch uri {
	case DriverSubmitTask:
		m := new(pb.DriverSubmitTaskRequest)
		return m, proto.Unmarshal(body, m)
	case SchedulerSubmitTaskResp:
		m := new(pb.SchedulerSubmitTaskResponse)
		return m, proto.Unmarshal(body, m)
	case RayRegisterTaskToGCS:
		m := new(pb.RegisterTaskToGCSRequest)
		return m, proto.Unmarshal(body, m)
	case RayRegisterTaskToGCSResp:
		m := new(pb.RegisterTaskToGCSResponse)
		return m, proto.Unmarshal(body, m)
	case RayFetchTask:
		m := new(pb.FetchTaskRequest)
		return m, proto.Unmarshal(body, m)
	case RayFetchTaskResp:
		m := new(pb.FetchTaskResponse)
		return m, proto.Unmarshal(body, m)
	case RayGetObjectLocation:
		m := new(pb.GetObjectLocationRequest)
		return m, proto.Unmarshal(body, m)
	case RayGetObjectLocationResp:
		m := new(pb.GetObjectLocationResponse)
		return m, proto.Unmarshal(body, m)
	case RayReportTaskResult:
		m := new(pb.ReportTaskResultRequest)
		return m, proto.Unmarshal(body, m)
	case RayReportTaskResultResp:
		m := new(pb.ReportTaskResultResponse)
		return m, proto.Unmarshal(body, m)
	case RayGetTaskResult:
		m := new(pb.GetTaskResultRequest)
		return m, proto.Unmarshal(body, m)
	case RayGetTaskResultResp:
		m := new(pb.GetTaskResultResponse)
		return m, proto.Unmarshal(body, m)
	default:
		return nil, fmt.Errorf("codec: not a ray uri %d", uri)
	}
}
