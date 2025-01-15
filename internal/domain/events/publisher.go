package events

import (
	"context"
)

// EventPublisher는 이벤트를 발행하는 인터페이스입니다.
type EventPublisher interface {
	// Publish는 단일 이벤트를 발행합니다.
	Publish(ctx context.Context, event Event) error

	// PublishAll은 여러 이벤트를 하나의 트랜잭션으로 발행합니다.
	PublishAll(ctx context.Context, events []Event) error
}
