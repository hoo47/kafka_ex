package events

import (
	pkgevents "github.com/hoo47/kafka_ex/pkg/events"
)

type AppInstallEvent struct {
	BaseEvent
}

func NewAppInstallEvent(aggregateID string, protoMsg *pkgevents.AppInstallEvent) AppInstallEvent {
	return AppInstallEvent{
		BaseEvent: NewBaseEvent("app", aggregateID, "AppInstallEvent", protoMsg),
	}
}

type AppUninstallEvent struct {
	BaseEvent
}

func NewAppUninstallEvent(aggregateID string, protoMsg *pkgevents.AppUninstallEvent) AppUninstallEvent {
	return AppUninstallEvent{
		BaseEvent: NewBaseEvent("app", aggregateID, "AppUninstallEvent", protoMsg),
	}
}
