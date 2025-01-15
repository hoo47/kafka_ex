package outbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/hoo47/kafka_ex/internal/domain/events"
	"github.com/hoo47/kafka_ex/internal/schema"
)

type OutboxEventPublisher struct {
	db    *sql.DB
	codec *schema.Codec
}

func NewOutboxEventPublisher(db *sql.DB, codec *schema.Codec) *OutboxEventPublisher {
	return &OutboxEventPublisher{
		db:    db,
		codec: codec,
	}
}

func (p *OutboxEventPublisher) Publish(ctx context.Context, event events.Event) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := p.saveEvent(ctx, tx, event); err != nil {
		return err
	}

	return tx.Commit()
}

func (p *OutboxEventPublisher) PublishAll(ctx context.Context, events []events.Event) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, event := range events {
		if err := p.saveEvent(ctx, tx, event); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (p *OutboxEventPublisher) saveEvent(ctx context.Context, tx *sql.Tx, event events.Event) error {
	// Proto 메시지로 변환
	protoMsg := event.ToProto()

	// Schema Registry 형식으로 직렬화
	payload, err := p.codec.Serialize(event.Type(), protoMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	query := `
        INSERT INTO event_outbox (id, aggregate_type, aggregate_id, type, payload)
        VALUES ($1, $2, $3, $4, $5)
    `

	_, err = tx.ExecContext(ctx, query,
		uuid.New(),
		event.AggregateType(),
		event.AggregateID(),
		event.Type(),
		payload,
	)
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	return nil
}
