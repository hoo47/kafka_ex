package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/hoo47/kafka_ex/internal/events"
	"github.com/hoo47/kafka_ex/internal/events/handlers"
	"github.com/hoo47/kafka_ex/internal/kafka"
	pkgevents "github.com/hoo47/kafka_ex/pkg/events"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Kafka 설정
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 이벤트 라우터 설정
	router := events.NewEventRouter(logger)

	// 핸들러 등록
	router.RegisterHandler(
		handlers.NewAppInstallHandler(logger),
		&pkgevents.AppInstallEvent{},
	)
	router.RegisterHandler(
		handlers.NewAppUninstallHandler(logger),
		&pkgevents.AppUninstallEvent{},
	)

	// Consumer 생성
	consumer := kafka.NewConsumer(router, logger)

	// Consumer 그룹 생성
	group, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "app-events-group", config)
	if err != nil {
		logger.Error("Error creating consumer group", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 시그널 처리
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	topics := []string{"app.events"}

	go func() {
		for {
			if err := group.Consume(ctx, topics, consumer); err != nil {
				logger.Error("Error from consumer", "error", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-signals
	logger.Info("Shutting down")
}
