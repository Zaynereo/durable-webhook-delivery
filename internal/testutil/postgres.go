package testutil

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/zaynereo/durable-webhook-delivery/internal/migrate"
	"github.com/zaynereo/durable-webhook-delivery/internal/store"
)

// integrationMu serializes Postgres integration tests across packages (go test runs packages in parallel).
var integrationMu sync.Mutex

func DatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	return url
}

func NewStore(t *testing.T) *store.Store {
	t.Helper()
	integrationMu.Lock()
	t.Cleanup(integrationMu.Unlock)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	url := DatabaseURL(t)
	if err := migrate.Up(url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := store.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	st := store.New(pool)
	if err := st.WaitHealthy(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := acquireDBTestLock(ctx, st); err != nil {
		t.Fatalf("advisory lock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = st.Pool().Exec(context.Background(), `SELECT pg_advisory_unlock(88001)`)
	})

	truncate(t, st)
	t.Cleanup(func() { truncate(t, st) })
	return st
}

func acquireDBTestLock(ctx context.Context, st *store.Store) error {
	_, err := st.Pool().Exec(ctx, `SELECT pg_advisory_lock(88001)`)
	return err
}

func truncate(t *testing.T, st *store.Store) {
	t.Helper()
	ctx := context.Background()
	_, err := st.Pool().Exec(ctx, `
		TRUNCATE webhook_dead_letters, outbox_events, delivery_attempts, webhooks RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
