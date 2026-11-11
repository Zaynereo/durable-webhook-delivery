package delivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/zaynereo/durable-webhook-delivery/internal/config"
	"github.com/zaynereo/durable-webhook-delivery/internal/destination"
	"github.com/zaynereo/durable-webhook-delivery/internal/hmacsig"
	"github.com/zaynereo/durable-webhook-delivery/internal/metrics"
	"github.com/zaynereo/durable-webhook-delivery/internal/store"
)

type Worker struct {
	cfg       *config.Config
	store     *store.Store
	allowlist *destination.Allowlist
	client    *http.Client
	log       *slog.Logger
}

func NewWorker(cfg *config.Config, st *store.Store, allowlist *destination.Allowlist, log *slog.Logger) *Worker {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableKeepAlives = false
	return &Worker{
		cfg:       cfg,
		store:     st,
		allowlist: allowlist,
		client: &http.Client{
			Timeout:   cfg.DeliveryTimeout,
			Transport: tr,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		log: log,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if n, err := w.store.RecoverExpiredLeases(ctx); err != nil {
			w.log.Error("recover leases", "err", err)
		} else if n > 0 {
			w.log.Info("recovered expired leases", "count", n)
		}

		claimed, err := w.store.ClaimPending(ctx, w.cfg.WorkerID, w.cfg.LeaseDuration, w.cfg.BatchSize)
		if err != nil {
			w.log.Error("claim", "err", err)
		} else {
			for _, c := range claimed {
				metrics.InFlight.Inc()
				w.deliverOne(ctx, c)
				metrics.InFlight.Dec()
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// DeliverForTest exposes a single delivery for integration tests.
func (w *Worker) DeliverForTest(ctx context.Context, c store.ClaimedDelivery) {
	w.deliverOne(ctx, c)
}

func (w *Worker) deliverOne(ctx context.Context, c store.ClaimedDelivery) {
	if err := w.allowlist.Validate(c.DestinationURL); err != nil {
		msg := err.Error()
		status := 0
		_ = w.store.FinishAttempt(ctx, store.FinishInput{
			AttemptID:     c.AttemptID,
			WebhookID:     c.WebhookID,
			AttemptNumber: c.AttemptNumber,
			HTTPStatus:    &status,
			ErrorMessage:  &msg,
			Success:       false,
			NextRetryAt:   nil,
		}, w.cfg.MaxDeliveryAttempts)
		metrics.DeliveryAttempts.WithLabelValues("rejected_destination").Inc()
		return
	}

	body := c.Payload
	ts := hmacsig.NowUnix()
	sig := hmacsig.Sign(w.cfg.HMACSecret, ts, body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.DestinationURL, bytes.NewReader(body))
	if err != nil {
		w.finishFailure(ctx, c, nil, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hmacsig.HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(hmacsig.HeaderSignature, sig)

	resp, err := w.client.Do(req)
	if err != nil {
		w.finishFailure(ctx, c, nil, err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		w.finishSuccess(ctx, c, resp.StatusCode)
		return
	}
	err = fmt.Errorf("unexpected status %d", resp.StatusCode)
	w.finishFailure(ctx, c, &resp.StatusCode, err)
}

func (w *Worker) finishSuccess(ctx context.Context, c store.ClaimedDelivery, httpStatus int) {
	err := w.store.FinishAttempt(ctx, store.FinishInput{
		AttemptID:     c.AttemptID,
		WebhookID:     c.WebhookID,
		AttemptNumber: c.AttemptNumber,
		HTTPStatus:    &httpStatus,
		Success:       true,
	}, w.cfg.MaxDeliveryAttempts)
	if err != nil {
		w.log.Error("finish success", "webhook_id", c.WebhookID, "err", err)
		return
	}
	_ = w.store.MarkOutboxProcessed(ctx, c.WebhookID)
	metrics.DeliveryAttempts.WithLabelValues("success").Inc()
	w.log.Info("delivered", "webhook_id", c.WebhookID, "attempt", c.AttemptNumber)
}

func (w *Worker) finishFailure(ctx context.Context, c store.ClaimedDelivery, httpStatus *int, deliveryErr error) {
	msg := deliveryErr.Error()
	var next *time.Time
	if c.AttemptNumber < w.cfg.MaxDeliveryAttempts {
		backoff := store.Backoff(c.AttemptNumber, w.cfg.InitialBackoff, w.cfg.MaxBackoff)
		t := time.Now().UTC().Add(backoff)
		next = &t
	}
	err := w.store.FinishAttempt(ctx, store.FinishInput{
		AttemptID:     c.AttemptID,
		WebhookID:     c.WebhookID,
		AttemptNumber: c.AttemptNumber,
		HTTPStatus:    httpStatus,
		ErrorMessage:  &msg,
		Success:       false,
		NextRetryAt:   next,
	}, w.cfg.MaxDeliveryAttempts)
	if err != nil {
		w.log.Error("finish failure", "webhook_id", c.WebhookID, "err", err)
		return
	}
	if c.AttemptNumber >= w.cfg.MaxDeliveryAttempts {
		metrics.DeadLetters.Inc()
		metrics.DeliveryAttempts.WithLabelValues("dead_letter").Inc()
		w.log.Warn("dead letter", "webhook_id", c.WebhookID, "attempt", c.AttemptNumber)
	} else {
		metrics.DeliveryAttempts.WithLabelValues("retry").Inc()
		w.log.Info("delivery failed, scheduled retry", "webhook_id", c.WebhookID, "attempt", c.AttemptNumber, "err", msg)
	}
}
