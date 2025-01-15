package schema

import (
	"testing"

	pkgevents "github.com/hoo47/kafka_ex/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestCodec_SerializeDeserialize(t *testing.T) {
	// Mock Schema Registry 설정
	registry := NewMockSchemaRegistry()
	registry.RegisterSchema("AppInstallEvent", 1, &pkgevents.AppInstallEvent{})

	codec := NewCodec(registry)

	// 테스트 케이스 정의
	testEvent := &pkgevents.AppInstallEvent{
		AppId:     "test-app-123",
		ChannelId: "channel-456",
		ManagerId: "manager-789",
	}

	// 직렬화
	serialized, err := codec.Serialize("AppInstallEvent", testEvent)
	require.NoError(t, err)

	// 기본 검증
	assert.Equal(t, byte(0x0), serialized[0], "Magic byte should be 0x0")
	assert.Equal(t, 4, len(serialized[1:5]), "Schema ID should be 4 bytes")

	// 역직렬화
	deserialized, err := codec.Deserialize(serialized, "AppInstallEvent")
	require.NoError(t, err)

	// 타입 검증
	deserializedEvent, ok := deserialized.(*pkgevents.AppInstallEvent)
	require.True(t, ok, "Deserialized message should be of type AppInstallEvent")

	// 값 검증
	assert.Equal(t, testEvent.AppId, deserializedEvent.AppId)
	assert.Equal(t, testEvent.ChannelId, deserializedEvent.ChannelId)
	assert.Equal(t, testEvent.ManagerId, deserializedEvent.ManagerId)
}

func TestCodec_SerializeErrors(t *testing.T) {
	registry := NewMockSchemaRegistry()
	codec := NewCodec(registry)

	tests := []struct {
		name      string
		eventType string
		input     proto.Message
		wantErr   string
	}{
		{
			name:      "Unknown event type",
			eventType: "UnknownEvent",
			input:     &pkgevents.AppInstallEvent{},
			wantErr:   "schema not found for event type",
		},
		{
			name:      "Nil message",
			eventType: "AppInstallEvent",
			input:     nil,
			wantErr:   "message is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := codec.Serialize(tt.eventType, tt.input)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestCodec_DeserializeErrors(t *testing.T) {
	registry := NewMockSchemaRegistry()
	registry.RegisterSchema("AppInstallEvent", 1, &pkgevents.AppInstallEvent{})
	codec := NewCodec(registry)

	tests := []struct {
		name      string
		input     []byte
		eventType string
		wantErr   string
	}{
		{
			name:      "Message too short",
			input:     []byte{0x0, 0x0},
			eventType: "AppInstallEvent",
			wantErr:   "message too short",
		},
		{
			name:      "Invalid magic byte",
			input:     []byte{0x1, 0x0, 0x0, 0x0, 0x1},
			eventType: "AppInstallEvent",
			wantErr:   "invalid magic byte",
		},
		{
			name:      "Schema ID mismatch",
			input:     []byte{0x0, 0x0, 0x0, 0x0, 0x2},
			eventType: "AppInstallEvent",
			wantErr:   "schema ID mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := codec.Deserialize(tt.input, tt.eventType)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
