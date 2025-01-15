CREATE TABLE event_outbox (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- 동시성 제어를 위한 인덱스
    CONSTRAINT uk_event_payload UNIQUE (aggregate_type, aggregate_id, payload),
    INDEX idx_unpublished (published) WHERE published = FALSE
); 