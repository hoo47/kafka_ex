package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hoo47/kafka_ex/pkg/events"
	"google.golang.org/protobuf/proto"
)

type AppInstallHandler struct {
	logger *slog.Logger
}

func NewAppInstallHandler(logger *slog.Logger) *AppInstallHandler {
	return &AppInstallHandler{logger: logger}
}

func (h *AppInstallHandler) EventType() string {
	return "AppInstallEvent"
}

func (h *AppInstallHandler) Handle(ctx context.Context, msg proto.Message) error {
	event, ok := msg.(*events.AppInstallEvent)
	if !ok {
		return fmt.Errorf("invalid event type: expected AppInstallEvent")
	}

	h.logger.Info("handling app install event",
		"app_id", event.AppId,
		"channel_id", event.ChannelId,
		"manager_id", event.ManagerId)

	return nil
}
