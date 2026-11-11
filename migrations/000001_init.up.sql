CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT webhooks_idempotency_key_unique UNIQUE (idempotency_key)
);

CREATE INDEX webhooks_status_idx ON webhooks (status);

CREATE TABLE delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    webhook_id UUID NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    leased_by TEXT,
    lease_expires_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    http_status INT,
    error_message TEXT,
    next_retry_at TIMESTAMPTZ,
    CONSTRAINT delivery_attempts_webhook_attempt_unique UNIQUE (webhook_id, attempt_number)
);

CREATE INDEX delivery_attempts_pending_retry_idx
    ON delivery_attempts (webhook_id)
    WHERE finished_at IS NULL OR (finished_at IS NOT NULL AND next_retry_at IS NOT NULL);

CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    webhook_id UUID NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL DEFAULT 'webhook_enqueued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX outbox_events_unprocessed_idx ON outbox_events (id) WHERE processed_at IS NULL;

CREATE TABLE webhook_dead_letters (
    webhook_id UUID PRIMARY KEY REFERENCES webhooks(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    last_error TEXT,
    attempt_count INT NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
