package schema

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// MockSchemaRegistry implements Registry interface for testing
type MockSchemaRegistry struct {
	schemas map[string]struct {
		id        int
		prototype proto.Message
	}
}

func NewMockSchemaRegistry() *MockSchemaRegistry {
	return &MockSchemaRegistry{
		schemas: make(map[string]struct {
			id        int
			prototype proto.Message
		}),
	}
}

func (r *MockSchemaRegistry) RegisterSchema(eventType string, id int, prototype proto.Message) {
	r.schemas[eventType] = struct {
		id        int
		prototype proto.Message
	}{
		id:        id,
		prototype: prototype,
	}
}

func (r *MockSchemaRegistry) GetSchemaInfo(eventType string) (int, proto.Message, error) {
	schema, ok := r.schemas[eventType]
	if !ok {
		return 0, nil, fmt.Errorf("schema not found for event type: %s", eventType)
	}
	return schema.id, proto.Clone(schema.prototype), nil
}
