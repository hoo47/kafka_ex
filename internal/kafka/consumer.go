package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/hoo47/kafka_ex/internal/events"
	"github.com/hoo47/kafka_ex/internal/schema"
)

type Consumer struct {
	router *events.EventRouter
	logger *slog.Logger
	codec  *schema.Codec
}

func NewConsumer(router *events.EventRouter, logger *slog.Logger, codec *schema.Codec) *Consumer {
	return &Consumer{
		router: router,
		logger: logger,
		codec:  codec,
	}
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := c.handleMessage(context.Background(), msg); err != nil {
			c.logger.Error("failed to handle message",
				"error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset)
			continue
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) handleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	eventType := getHeaderValue(msg.Headers, "type")
	if eventType == "" {
		return fmt.Errorf("message missing type header")
	}

	// Schema Registry 형식으로 역직렬화
	event, err := c.codec.Deserialize(msg.Value, eventType)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %w", err)
	}

	return c.router.HandleMessage(ctx, eventType, event)
}

func getHeaderValue(headers []*sarama.RecordHeader, key string) string {
	for _, header := range headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}
