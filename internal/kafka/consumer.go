package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/hoo47/kafka_ex/internal/events"
)

type Consumer struct {
	router *events.EventRouter
	logger *slog.Logger
}

func NewConsumer(router *events.EventRouter, logger *slog.Logger) *Consumer {
	return &Consumer{
		router: router,
		logger: logger,
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

	return c.router.HandleMessage(ctx, eventType, msg.Value)
}

func getHeaderValue(headers []*sarama.RecordHeader, key string) string {
	for _, header := range headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}
