package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/lib/pq"

	"github.com/hoo47/kafka_ex/internal/config"
	"github.com/hoo47/kafka_ex/internal/domain/events"
	"github.com/hoo47/kafka_ex/internal/infrastructure/outbox"
	"github.com/hoo47/kafka_ex/internal/schema"
	pkgevents "github.com/hoo47/kafka_ex/pkg/events"
)

func main() {
	// 설정 로드
	cfg, err := config.Load("config/config.yml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// PostgreSQL 연결
	db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/dbname?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Schema Registry 설정
	registry := schema.NewSchemaRegistry(cfg.SchemaRegistry.URL)
	registry.RegisterPrototype("AppInstallEvent", &pkgevents.AppInstallEvent{})
	registry.RegisterPrototype("AppUninstallEvent", &pkgevents.AppUninstallEvent{})

	subjects := map[string]string{
		"AppInstallEvent":   cfg.SchemaRegistry.Subjects.AppInstall,
		"AppUninstallEvent": cfg.SchemaRegistry.Subjects.AppUninstall,
	}
	if err := registry.RegisterSchemas(subjects); err != nil {
		log.Fatalf("Failed to register schemas: %v", err)
	}

	// Codec 생성
	codec := schema.NewCodec(registry)

	// 이벤트 퍼블리셔 생성
	publisher := outbox.NewOutboxEventPublisher(db, codec)

	// 앱 설치 이벤트 생성
	installEvent := pkgevents.AppInstallEvent{
		AppId:     "app123",
		ChannelId: "channel456",
		ManagerId: "manager789",
	}

	// 도메인 이벤트로 변환
	domainEvent := events.NewAppInstallEvent("app123", &installEvent)

	// 이벤트 발행
	ctx := context.Background()
	if err := publisher.Publish(ctx, domainEvent); err != nil {
		log.Fatalf("Failed to publish event: %v", err)
	}

	log.Println("Event published successfully")

	// 여러 이벤트 동시 발행 예시
	uninstallEvent := pkgevents.AppUninstallEvent{
		AppId:     "app123",
		ChannelId: "channel456",
		ManagerId: "manager789",
	}

	multiEvents := []events.Event{
		domainEvent,
		events.NewAppUninstallEvent("app123", &uninstallEvent),
	}

	if err := publisher.PublishAll(ctx, multiEvents); err != nil {
		log.Fatalf("Failed to publish multiple events: %v", err)
	}

	log.Println("Multiple events published successfully")
}
