package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zaynereo/durable-webhook-delivery/internal/store"
	"github.com/zaynereo/durable-webhook-delivery/internal/testutil"
)

func TestEnqueueIdempotency(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	payload := json.RawMessage(`{"event_id":"e1"}`)
	in := store.EnqueueInput{
		IdempotencyKey: "key-1",
		DestinationURL: "http://sink:9090/webhook",
		Payload:        payload,
	}
	id1, created, err := st.Enqueue(ctx, in)
	if err != nil || !created || id1 == "" {
		t.Fatalf("first enqueue: id=%s created=%v err=%v", id1, created, err)
	}
	id2, created2, err := st.Enqueue(ctx, in)
	if err != nil || created2 || id2 != id1 {
		t.Fatalf("duplicate enqueue: id=%s created=%v err=%v", id2, created2, err)
	}
	in2 := in
	in2.Payload = json.RawMessage(`{"event_id":"e2"}`)
	_, _, err = st.Enqueue(ctx, in2)
	if err != store.ErrIdempotencyConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestConcurrentClaimsSkipLocked(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, _, err := st.Enqueue(ctx, store.EnqueueInput{
			IdempotencyKey: fmt.Sprintf("k-%d", i),
			DestinationURL: "http://sink:9090/webhook",
			Payload:        json.RawMessage(`{"n":1}`),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	claimed := make(chan string, 20)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch, err := st.ClaimPending(ctx, "w", 30*time.Second, 3)
			if err != nil {
				t.Error(err)
				return
			}
			for _, c := range batch {
				claimed <- c.WebhookID
			}
		}()
	}
	wg.Wait()
	close(claimed)

	seen := map[string]int{}
	for id := range claimed {
		seen[id]++
		if seen[id] > 1 {
			t.Fatalf("webhook %s claimed twice concurrently", id)
		}
	}
	if len(seen) == 0 {
		t.Fatal("expected some claims")
	}
}

func TestExpiredLeaseRecovery(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	id, _, err := st.Enqueue(ctx, store.EnqueueInput{
		IdempotencyKey: "lease-1",
		DestinationURL: "http://sink:9090/webhook",
		Payload:        json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := st.ClaimPending(ctx, "w1", 50*time.Millisecond, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(claimed))
	}
	time.Sleep(120 * time.Millisecond)
	n, err := st.RecoverExpiredLeases(ctx)
	if err != nil || n != 1 {
		t.Fatalf("recover: n=%d err=%v", n, err)
	}
	claimed2, err := st.ClaimPending(ctx, "w2", time.Second, 5)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range claimed2 {
		if c.WebhookID == id {
			found = true
		}
	}
	if !found {
		t.Fatal("expected webhook reclaim after lease expiry")
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	st := testutil.NewStore(t)
	ctx := context.Background()
	id, _, err := st.Enqueue(ctx, store.EnqueueInput{
		IdempotencyKey: "dl-1",
		DestinationURL: "http://sink:9090/webhook",
		Payload:        json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	const max = 3
	for attempt := 1; attempt <= max; attempt++ {
		claimed, err := st.ClaimPending(ctx, "w", time.Second, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim attempt %d: %v", attempt, err)
		}
		c := claimed[0]
		msg := "fail"
		var next *time.Time
		if attempt < max {
			tm := time.Now().UTC().Add(-time.Second)
			next = &tm
		}
		err = st.FinishAttempt(ctx, store.FinishInput{
			AttemptID:     c.AttemptID,
			WebhookID:     c.WebhookID,
			AttemptNumber: c.AttemptNumber,
			ErrorMessage:  &msg,
			Success:       false,
			NextRetryAt:   next,
		}, max)
		if err != nil {
			t.Fatal(err)
		}
	}
	wh, err := st.GetWebhook(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if wh.Status != store.StatusDeadLetter {
		t.Fatalf("status=%s", wh.Status)
	}
}
