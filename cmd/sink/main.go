package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/zaynereo/durable-webhook-delivery/internal/hmacsig"
)

// Demo sink: verifies HMAC and optionally fails or delays via headers (test hooks).
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	secret := os.Getenv("HMAC_SECRET")
	if secret == "" {
		secret = "dev-hmac-secret-change-me"
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":9090"
	}

	var mu sync.Mutex
	received := make(map[string]int)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		tsStr := r.Header.Get(hmacsig.HeaderTimestamp)
		sig := r.Header.Get(hmacsig.HeaderSignature)
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil || !hmacsig.Verify(secret, sig, ts, body) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		if r.Header.Get("X-Test-Fail") == "1" {
			http.Error(w, "forced failure", http.StatusInternalServerError)
			return
		}

		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		key, _ := payload["event_id"].(string)
		if key != "" {
			mu.Lock()
			received[key]++
			mu.Unlock()
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /received/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		mu.Lock()
		count := received[id]
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]int{"count": count})
	})

	log.Info("sink listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}
