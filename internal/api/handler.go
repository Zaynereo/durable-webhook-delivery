package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zaynereo/durable-webhook-delivery/internal/destination"
	"github.com/zaynereo/durable-webhook-delivery/internal/metrics"
	"github.com/zaynereo/durable-webhook-delivery/internal/store"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type Server struct {
	store     *store.Store
	allowlist *destination.Allowlist
	mux       *http.ServeMux
}

func NewServer(st *store.Store, allowlist *destination.Allowlist) *Server {
	s := &Server{store: st, allowlist: allowlist, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /metrics", promhttp.Handler().ServeHTTP)
	s.mux.HandleFunc("POST /v1/webhooks", s.handleEnqueue)
	s.mux.HandleFunc("GET /v1/webhooks/{id}", s.handleGetStatus)
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Pool().Ping(r.Context()); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

type enqueueRequest struct {
	DestinationURL string          `json:"destination_url"`
	Payload        json.RawMessage `json:"payload"`
}

type enqueueResponse struct {
	ID      string `json:"id"`
	Created bool   `json:"created"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	idem := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idem == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}
	if len(idem) > 256 {
		http.Error(w, "Idempotency-Key too long", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()

	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.DestinationURL == "" {
		http.Error(w, "destination_url is required", http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		http.Error(w, "payload must be valid JSON", http.StatusBadRequest)
		return
	}

	if err := s.allowlist.Validate(req.DestinationURL); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	id, created, err := s.store.Enqueue(ctx, store.EnqueueInput{
		IdempotencyKey: idem,
		DestinationURL: req.DestinationURL,
		Payload:        req.Payload,
	})
	if errors.Is(err, store.ErrIdempotencyConflict) {
		metrics.EnqueueTotal.WithLabelValues("conflict").Inc()
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		metrics.EnqueueTotal.WithLabelValues("error").Inc()
		http.Error(w, "enqueue failed", http.StatusInternalServerError)
		return
	}

	if created {
		metrics.EnqueueTotal.WithLabelValues("created").Inc()
	} else {
		metrics.EnqueueTotal.WithLabelValues("duplicate").Inc()
	}

	w.Header().Set("Content-Type", "application/json")
	if created {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(enqueueResponse{ID: id, Created: created})
}

func (s *Server) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	wh, err := s.store.GetWebhook(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "lookup failed", http.StatusInternalServerError)
		return
	}

	type statusResponse struct {
		ID               string          `json:"id"`
		Status           string          `json:"status"`
		IdempotencyKey   string          `json:"idempotency_key"`
		DestinationURL   string          `json:"destination_url"`
		AttemptCount     int             `json:"attempt_count"`
		LastError        *string         `json:"last_error,omitempty"`
		DeadLetterReason *string         `json:"dead_letter_reason,omitempty"`
		Payload          json.RawMessage `json:"payload"`
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(statusResponse{
		ID:               wh.ID,
		Status:           string(wh.Status),
		IdempotencyKey:   wh.IdempotencyKey,
		DestinationURL:   wh.DestinationURL,
		AttemptCount:     wh.AttemptCount,
		LastError:        wh.LastError,
		DeadLetterReason: wh.DeadLetterReason,
		Payload:          wh.Payload,
	})
}

// DrainBody is used by tests.
func DrainBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, maxBodyBytes))
}
