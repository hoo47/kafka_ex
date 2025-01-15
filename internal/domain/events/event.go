package events

import (
	"google.golang.org/protobuf/proto"
)

// Event는 도메인 이벤트를 나타내는 인터페이스입니다.
type Event interface {
	AggregateType() string
	AggregateID() string
	Type() string
	ToProto() proto.Message
}

// BaseEvent는 모든 이벤트의 기본 구현을 제공합니다.
type BaseEvent struct {
	aggregateType string
	aggregateID   string
	eventType     string
	protoMsg      proto.Message
}

func NewBaseEvent(aggregateType, aggregateID, eventType string, protoMsg proto.Message) BaseEvent {
	return BaseEvent{
		aggregateType: aggregateType,
		aggregateID:   aggregateID,
		eventType:     eventType,
		protoMsg:      protoMsg,
	}
}

func (e BaseEvent) AggregateType() string {
	return e.aggregateType
}

func (e BaseEvent) AggregateID() string {
	return e.aggregateID
}

func (e BaseEvent) Type() string {
	return e.eventType
}

func (e BaseEvent) ToProto() proto.Message {
	return e.protoMsg
}
