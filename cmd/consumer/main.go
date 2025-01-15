package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/hoo47/kafka_ex/internal/config"
	"github.com/hoo47/kafka_ex/internal/events"
	"github.com/hoo47/kafka_ex/internal/events/handlers"
	"github.com/hoo47/kafka_ex/internal/kafka"
	"github.com/hoo47/kafka_ex/internal/schema"
	pkgevents "github.com/hoo47/kafka_ex/pkg/events"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// 설정 로드
	cfg, err := config.Load("config/config.yml")
	if err != nil {
		logger.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	// Schema Registry 설정
	registry := schema.NewSchemaRegistry(cfg.SchemaRegistry.URL)
	registry.RegisterPrototype("AppInstallEvent", &pkgevents.AppInstallEvent{})
	registry.RegisterPrototype("AppUninstallEvent", &pkgevents.AppUninstallEvent{})

	subjects := map[string]string{
		"AppInstallEvent":   cfg.SchemaRegistry.Subjects.AppInstall,
		"AppUninstallEvent": cfg.SchemaRegistry.Subjects.AppUninstall,
	}
	if err := registry.RegisterSchemas(subjects); err != nil {
		logger.Error("Failed to register schemas", "error", err)
		os.Exit(1)
	}

	// Codec 생성
	codec := schema.NewCodec(registry)

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
	consumer := kafka.NewConsumer(router, logger, codec)

	// Consumer 그룹 생성
	group, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.Consumer.GroupID, config)
	if err != nil {
		logger.Error("Error creating consumer group", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 시그널 처리
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	topics := []string{cfg.Kafka.Topics.AppEvents}

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
