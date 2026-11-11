package delivery_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/zaynereo/durable-webhook-delivery/internal/config"
	"github.com/zaynereo/durable-webhook-delivery/internal/delivery"
	"github.com/zaynereo/durable-webhook-delivery/internal/destination"
	"github.com/zaynereo/durable-webhook-delivery/internal/hmacsig"
	"github.com/zaynereo/durable-webhook-delivery/internal/store"
	"github.com/zaynereo/durable-webhook-delivery/internal/testutil"
)

func testCfg(url string) *config.Config {
	return &config.Config{
		HMACSecret:          "test-hmac-secret",
		AllowedDestinations: []string{url},
		WorkerID:            "test-worker",
		MaxDeliveryAttempts: 3,
		InitialBackoff:      10 * time.Millisecond,
		MaxBackoff:          50 * time.Millisecond,
		DeliveryTimeout:     2 * time.Second,
		LeaseDuration:       5 * time.Second,
		PollInterval:        50 * time.Millisecond,
		BatchSize:           4,
	}
}

func TestWorkerDeliverSuccessHMAC(t *testing.T) {
	secret := "test-hmac-secret"
	var gotSig bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ts, _ := strconv.ParseInt(r.Header.Get(hmacsig.HeaderTimestamp), 10, 64)
		gotSig = hmacsig.Verify(secret, r.Header.Get(hmacsig.HeaderSignature), ts, body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dest := srv.URL + "/"

	al, err := destination.NewAllowlist([]string{dest})
	if err != nil {
		t.Fatal(err)
	}
	st := testutil.NewStore(t)
	ctx := context.Background()
	id, _, err := st.Enqueue(ctx, store.EnqueueInput{
		IdempotencyKey: "wh-1",
		DestinationURL: dest,
		Payload:        json.RawMessage(`{"event_id":"evt-1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	w := delivery.NewWorker(testCfg(dest), st, al, slog.Default())
	claimed, err := st.ClaimPending(ctx, "test-worker", 5*time.Second, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatal(err)
	}
	w.DeliverForTest(ctx, claimed[0])

	if !gotSig {
		t.Fatal("sink did not receive valid HMAC")
	}
	wh, err := st.GetWebhook(ctx, id)
	if err != nil || wh.Status != store.StatusDelivered {
		t.Fatalf("status=%s err=%v", wh.Status, err)
	}
}

func TestWorkerRetryThenDeadLetter(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()
	dest := srv.URL + "/"

	al, _ := destination.NewAllowlist([]string{dest})
	st := testutil.NewStore(t)
	ctx := context.Background()
	id, _, _ := st.Enqueue(ctx, store.EnqueueInput{
		IdempotencyKey: "wh-fail",
		DestinationURL: dest,
		Payload:        json.RawMessage(`{"event_id":"evt-fail"}`),
	})

	cfg := testCfg(dest)
	cfg.MaxDeliveryAttempts = 2
	cfg.InitialBackoff = 1 * time.Millisecond
	w := delivery.NewWorker(cfg, st, al, slog.Default())

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		claimed, err := st.ClaimPending(ctx, cfg.WorkerID, cfg.LeaseDuration, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) == 0 {
			time.Sleep(20 * time.Millisecond)
			wh, _ := st.GetWebhook(ctx, id)
			if wh.Status == store.StatusDeadLetter {
				break
			}
			continue
		}
		w.DeliverForTest(ctx, claimed[0])
	}
	wh, err := st.GetWebhook(ctx, id)
	if err != nil || wh.Status != store.StatusDeadLetter {
		t.Fatalf("expected dead_letter, got %s err=%v attempts=%d", wh.Status, err, attempts)
	}
	if attempts < 2 {
		t.Fatalf("expected at least 2 http attempts, got %d", attempts)
	}
}
