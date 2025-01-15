package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"
)

const (
	magicByte = 0x0
)

type Codec struct {
	registry Registry
}

func NewCodec(registry Registry) *Codec {
	return &Codec{
		registry: registry,
	}
}

// Serialize converts a Proto message to Schema Registry format
// Format: [Magic Byte][Schema ID][Proto Message]
func (c *Codec) Serialize(eventType string, msg proto.Message) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}

	schemaID, _, err := c.registry.GetSchemaInfo(eventType)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema info: %w", err)
	}

	// Proto 메시지를 바이트 배열로 직렬화
	protoBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	// 최종 메시지 생성을 위한 버퍼 준비
	buf := make([]byte, 0, 5+len(protoBytes))
	buffer := bytes.NewBuffer(buf)

	// Magic Byte 쓰기
	if err := buffer.WriteByte(magicByte); err != nil {
		return nil, fmt.Errorf("failed to write magic byte: %w", err)
	}

	// Schema ID 쓰기 (4 bytes, big-endian)
	if err := binary.Write(buffer, binary.BigEndian, uint32(schemaID)); err != nil {
		return nil, fmt.Errorf("failed to write schema id: %w", err)
	}

	// 직렬화된 Proto 메시지 쓰기
	if _, err := buffer.Write(protoBytes); err != nil {
		return nil, fmt.Errorf("failed to write message: %w", err)
	}

	return buffer.Bytes(), nil
}

// Deserialize converts Schema Registry format back to Proto message
func (c *Codec) Deserialize(data []byte, eventType string) (proto.Message, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("message too short")
	}

	// Magic Byte 확인
	if data[0] != magicByte {
		return nil, fmt.Errorf("invalid magic byte")
	}

	// Schema ID 읽기
	schemaID := binary.BigEndian.Uint32(data[1:5])

	// 스키마 정보 가져오기
	expectedID, prototype, err := c.registry.GetSchemaInfo(eventType)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema info: %w", err)
	}

	// Schema ID 검증
	if int(schemaID) != expectedID {
		return nil, fmt.Errorf("schema ID mismatch: expected %d, got %d", expectedID, schemaID)
	}

	// 메시지 복제 및 역직렬화
	msg := proto.Clone(prototype)
	if err := proto.Unmarshal(data[5:], msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return msg, nil
}
