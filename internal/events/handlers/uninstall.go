package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hoo47/kafka_ex/pkg/events"

	"google.golang.org/protobuf/proto"
)

type AppUninstallHandler struct {
	logger *slog.Logger
}

func NewAppUninstallHandler(logger *slog.Logger) *AppUninstallHandler {
	return &AppUninstallHandler{logger: logger}
}

func (h *AppUninstallHandler) EventType() string {
	return "AppUninstallEvent"
}

func (h *AppUninstallHandler) Handle(ctx context.Context, msg proto.Message) error {
	event, ok := msg.(*events.AppUninstallEvent)
	if !ok {
		return fmt.Errorf("invalid event type: expected AppUninstallEvent")
	}

	h.logger.Info("handling app uninstall event",
		"app_id", event.AppId,
		"device_id", event.DeviceId,
		"uninstalled_at", event.UninstalledAt,
		"reason", event.Reason)

	return nil
}
