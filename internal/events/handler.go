package events

import (
	"context"

	"google.golang.org/protobuf/proto"
)

type EventHandler interface {
	EventType() string
	Handle(context.Context, proto.Message) error
}
