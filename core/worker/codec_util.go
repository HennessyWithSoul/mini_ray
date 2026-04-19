package worker

import (
	"encoding/binary"
	"fmt"
	"math"
)

// DecodeArg 根据目标类型解码字节数据
type DecodeArg func([]byte) (any, error)

// DecodeInt 解码为 int (使用64位有符号整数编码)
func DecodeInt(data []byte) (any, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("data too short for int: need 8 bytes, got %d", len(data))
	}
	return int(binary.BigEndian.Uint64(data)), nil
}

// DecodeString 解码为字符串
func DecodeString(data []byte) (any, error) {
	return string(data), nil
}

// DecodeFloat64 解码为 float64
func DecodeFloat64(data []byte) (any, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("data too short for float64: need 8 bytes, got %d", len(data))
	}
	bits := binary.BigEndian.Uint64(data)
	return math.Float64frombits(bits), nil
}

// DecodeBool 解码为 bool (1字节)
func DecodeBool(data []byte) (any, error) {
	if len(data) < 1 {
		return false, fmt.Errorf("data too short for bool: need 1 byte, got %d", len(data))
	}
	return data[0] != 0, nil
}

// DecodeBytes 直接返回字节数组
func DecodeBytes(data []byte) (any, error) {
	return data, nil
}

// EncodeInt 将 int 编码为字节数组 (8字节大端序)
func EncodeInt(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// EncodeString 将字符串编码为字节数组
func EncodeString(s string) []byte {
	return []byte(s)
}

// EncodeFloat64 将 float64 编码为字节数组
func EncodeFloat64(f float64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(f))
	return b
}

// EncodeBool 将 bool 编码为字节数组
func EncodeBool(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}
