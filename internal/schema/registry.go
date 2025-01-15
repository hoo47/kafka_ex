package schema

import (
	"fmt"

	"github.com/riferrei/srclient"
	"google.golang.org/protobuf/proto"
)

// Registry defines the interface for schema registry operations
type Registry interface {
	GetSchemaInfo(eventType string) (int, proto.Message, error)
}

// SchemaRegistry implements Registry interface
type SchemaRegistry struct {
	client  *srclient.SchemaRegistryClient
	schemas map[string]struct {
		id        int
		prototype proto.Message
	}
}

func NewSchemaRegistry(url string) *SchemaRegistry {
	return &SchemaRegistry{
		client: srclient.CreateSchemaRegistryClient(url),
		schemas: make(map[string]struct {
			id        int
			prototype proto.Message
		}),
	}
}

// RegisterPrototype registers a prototype message for a given event type
func (r *SchemaRegistry) RegisterPrototype(eventType string, prototype proto.Message) {
	r.schemas[eventType] = struct {
		id        int
		prototype proto.Message
	}{
		id:        0, // ID will be set when fetching from Schema Registry
		prototype: prototype,
	}
}

// RegisterSchemas fetches schema IDs from Schema Registry for registered prototypes
func (r *SchemaRegistry) RegisterSchemas(subjects map[string]string) error {
	for eventType, subject := range subjects {
		schema, err := r.client.GetLatestSchema(subject)
		if err != nil {
			return fmt.Errorf("failed to get schema for subject %s: %w", subject, err)
		}

		if _, ok := r.schemas[eventType]; !ok {
			return fmt.Errorf("no prototype registered for event type: %s", eventType)
		}

		r.schemas[eventType] = struct {
			id        int
			prototype proto.Message
		}{
			id:        schema.ID(),
			prototype: proto.Clone(r.schemas[eventType].prototype),
		}
	}
	return nil
}

func (r *SchemaRegistry) GetSchemaInfo(eventType string) (int, proto.Message, error) {
	schema, ok := r.schemas[eventType]
	if !ok {
		return 0, nil, fmt.Errorf("schema not found for event type: %s", eventType)
	}
	return schema.id, proto.Clone(schema.prototype), nil
}
