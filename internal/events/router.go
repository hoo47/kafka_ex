package events

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"
)

type EventRouter struct {
	handlers map[string]struct {
		handler   EventHandler
		prototype proto.Message
	}
	logger *slog.Logger
}

func NewEventRouter(logger *slog.Logger) *EventRouter {
	return &EventRouter{
		handlers: make(map[string]struct {
			handler   EventHandler
			prototype proto.Message
		}),
		logger: logger,
	}
}

func (r *EventRouter) RegisterHandler(h EventHandler, prototype proto.Message) {
	r.handlers[h.EventType()] = struct {
		handler   EventHandler
		prototype proto.Message
	}{h, prototype}
}

func (r *EventRouter) HandleMessage(ctx context.Context, eventType string, msg proto.Message) error {
	registration, exists := r.handlers[eventType]
	if !exists {
		return fmt.Errorf("no handler registered for event type: %s", eventType)
	}

	r.logger.Info("handling event",
		"type", eventType,
		"handler", fmt.Sprintf("%T", registration.handler))

	return registration.handler.Handle(ctx, msg)
}
