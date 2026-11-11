package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key reused with different payload or destination")
	ErrNotFound            = errors.New("webhook not found")
)

type WebhookStatus string

const (
	StatusPending    WebhookStatus = "pending"
	StatusDelivering WebhookStatus = "delivering"
	StatusDelivered  WebhookStatus = "delivered"
	StatusFailed     WebhookStatus = "failed"
	StatusDeadLetter WebhookStatus = "dead_letter"
)

type Webhook struct {
	ID              string
	IdempotencyKey  string
	DestinationURL  string
	Payload         json.RawMessage
	Status          WebhookStatus
	CreatedAt       time.Time
	UpdatedAt       time.Time
	AttemptCount    int
	LastError       *string
	DeadLetterReason *string
}

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

type EnqueueInput struct {
	IdempotencyKey string
	DestinationURL string
	Payload        json.RawMessage
}

// Enqueue inserts webhook and outbox row in one transaction.
// Duplicate idempotency key with same destination+payload returns existing ID.
func (s *Store) Enqueue(ctx context.Context, in EnqueueInput) (id string, created bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)

	var existingID string
	var sameRequest bool
	err = tx.QueryRow(ctx, `
		SELECT id::text,
		       (destination_url = $2 AND payload = $3::jsonb)
		FROM webhooks WHERE idempotency_key = $1
	`, in.IdempotencyKey, in.DestinationURL, in.Payload).Scan(&existingID, &sameRequest)
	if err == nil {
		if !sameRequest {
			return "", false, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return "", false, err
		}
		return existingID, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO webhooks (idempotency_key, destination_url, payload, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id::text
	`, in.IdempotencyKey, in.DestinationURL, in.Payload, StatusPending).Scan(&id)
	if err != nil {
		return "", false, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (webhook_id, event_type)
		VALUES ($1::uuid, 'webhook_enqueued')
	`, id)
	if err != nil {
		return "", false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", false, err
	}
	return id, true, nil
}

func (s *Store) GetWebhook(ctx context.Context, id string) (*Webhook, error) {
	var w Webhook
	var lastErr *string
	var dlReason *string
	err := s.pool.QueryRow(ctx, `
		SELECT w.id::text, w.idempotency_key, w.destination_url, w.payload, w.status,
		       w.created_at, w.updated_at,
		       (SELECT COUNT(*)::int FROM delivery_attempts d WHERE d.webhook_id = w.id),
		       d.last_error, d.reason
		FROM webhooks w
		LEFT JOIN webhook_dead_letters d ON d.webhook_id = w.id
		WHERE w.id = $1::uuid
	`, id).Scan(
		&w.ID, &w.IdempotencyKey, &w.DestinationURL, &w.Payload, &w.Status,
		&w.CreatedAt, &w.UpdatedAt, &w.AttemptCount, &lastErr, &dlReason,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	w.LastError = lastErr
	w.DeadLetterReason = dlReason
	return &w, nil
}

type ClaimedDelivery struct {
	WebhookID      string
	AttemptID      int64
	AttemptNumber  int
	DestinationURL string
	Payload        json.RawMessage
}

// ClaimPending picks webhooks needing delivery with SKIP LOCKED and sets a lease.
func (s *Store) ClaimPending(ctx context.Context, workerID string, lease time.Duration, limit int) ([]ClaimedDelivery, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	leaseUntil := time.Now().UTC().Add(lease)

	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT w.id
			FROM webhooks w
			WHERE w.status NOT IN ('delivered', 'dead_letter')
			  AND NOT EXISTS (SELECT 1 FROM webhook_dead_letters dl WHERE dl.webhook_id = w.id)
			  AND NOT EXISTS (
			    SELECT 1 FROM delivery_attempts d
			    WHERE d.webhook_id = w.id
			      AND d.finished_at IS NULL
			      AND (d.lease_expires_at IS NULL OR d.lease_expires_at > NOW())
			  )
			  AND (
			    NOT EXISTS (SELECT 1 FROM delivery_attempts d2 WHERE d2.webhook_id = w.id)
			    OR (
			      SELECT d3.next_retry_at
			      FROM delivery_attempts d3
			      WHERE d3.webhook_id = w.id AND d3.finished_at IS NOT NULL
			      ORDER BY d3.attempt_number DESC
			      LIMIT 1
			    ) <= NOW()
			  )
			ORDER BY w.created_at
			FOR UPDATE OF w SKIP LOCKED
			LIMIT $1
		)
		SELECT w.id::text, w.destination_url, w.payload,
		       COALESCE((SELECT MAX(attempt_number) FROM delivery_attempts da WHERE da.webhook_id = w.id), 0) + 1 AS next_attempt
		FROM webhooks w
		INNER JOIN candidates c ON c.id = w.id
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type candidate struct {
		webhookID      string
		destinationURL string
		payload        json.RawMessage
		attemptNumber  int
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.webhookID, &c.destinationURL, &c.payload, &c.attemptNumber); err != nil {
			return nil, err
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	var claimed []ClaimedDelivery
	for _, c := range candidates {
		var cd ClaimedDelivery
		cd.WebhookID = c.webhookID
		cd.DestinationURL = c.destinationURL
		cd.Payload = c.payload
		cd.AttemptNumber = c.attemptNumber

		err := tx.QueryRow(ctx, `
			INSERT INTO delivery_attempts (webhook_id, attempt_number, leased_by, lease_expires_at)
			VALUES ($1::uuid, $2, $3, $4)
			RETURNING id
		`, cd.WebhookID, cd.AttemptNumber, workerID, leaseUntil).Scan(&cd.AttemptID)
		if err != nil {
			return nil, err
		}

		_, err = tx.Exec(ctx, `UPDATE webhooks SET status = $2, updated_at = NOW() WHERE id = $1::uuid`, cd.WebhookID, StatusDelivering)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, cd)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

type FinishInput struct {
	AttemptID     int64
	WebhookID     string
	AttemptNumber int
	HTTPStatus    *int
	ErrorMessage  *string
	Success       bool
	NextRetryAt   *time.Time
}

func (s *Store) FinishAttempt(ctx context.Context, in FinishInput, maxAttempts int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		UPDATE delivery_attempts
		SET finished_at = NOW(), http_status = $2, error_message = $3, next_retry_at = $4,
		    leased_by = NULL, lease_expires_at = NULL
		WHERE id = $1
	`, in.AttemptID, in.HTTPStatus, in.ErrorMessage, in.NextRetryAt)
	if err != nil {
		return err
	}

	if in.Success {
		_, err = tx.Exec(ctx, `UPDATE webhooks SET status = $2, updated_at = NOW() WHERE id = $1::uuid`, in.WebhookID, StatusDelivered)
		if err != nil {
			return err
		}
	} else if in.AttemptNumber >= maxAttempts {
		reason := "max_attempts_exceeded"
		msg := ""
		if in.ErrorMessage != nil {
			msg = *in.ErrorMessage
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO webhook_dead_letters (webhook_id, reason, last_error, attempt_count)
			VALUES ($1::uuid, $2, $3, $4)
			ON CONFLICT (webhook_id) DO UPDATE SET reason = EXCLUDED.reason, last_error = EXCLUDED.last_error, attempt_count = EXCLUDED.attempt_count, dead_lettered_at = NOW()
		`, in.WebhookID, reason, msg, in.AttemptNumber)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE webhooks SET status = $2, updated_at = NOW() WHERE id = $1::uuid`, in.WebhookID, StatusDeadLetter)
		if err != nil {
			return err
		}
	} else {
		_, err = tx.Exec(ctx, `UPDATE webhooks SET status = $2, updated_at = NOW() WHERE id = $1::uuid`, in.WebhookID, StatusFailed)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// RecoverExpiredLeases clears in-flight attempts whose lease expired so another worker can claim.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE delivery_attempts
		SET finished_at = NOW(),
		    error_message = 'lease_expired',
		    leased_by = NULL,
		    lease_expires_at = NULL,
		    next_retry_at = NOW()
		WHERE finished_at IS NULL
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at < NOW()
	`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) MarkOutboxProcessed(ctx context.Context, webhookID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events SET processed_at = NOW()
		WHERE webhook_id = $1::uuid AND processed_at IS NULL
	`, webhookID)
	return err
}

func Backoff(attempt int, initial, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := initial
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

func (s *Store) WaitHealthy(ctx context.Context) error {
	for {
		if err := s.pool.Ping(ctx); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		return nil
	}
}

func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return pool, nil
}
