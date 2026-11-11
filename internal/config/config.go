package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL         string
	ListenAddr          string
	HMACSecret          string
	AllowedDestinations []string
	WorkerID            string

	MaxDeliveryAttempts int
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	DeliveryTimeout     time.Duration
	LeaseDuration       time.Duration
	PollInterval        time.Duration
	BatchSize           int
}

func LoadFromEnv() (*Config, error) {
	c := &Config{
		DatabaseURL:         envOr("DATABASE_URL", "postgres://webhook:webhook@localhost:5432/webhooks?sslmode=disable"),
		ListenAddr:          envOr("LISTEN_ADDR", ":8080"),
		HMACSecret:          envOr("HMAC_SECRET", "dev-hmac-secret-change-me"),
		WorkerID:            envOr("WORKER_ID", "worker-1"),
		MaxDeliveryAttempts: envIntOr("MAX_DELIVERY_ATTEMPTS", 5),
		InitialBackoff:      envDurationOr("INITIAL_BACKOFF", 2*time.Second),
		MaxBackoff:          envDurationOr("MAX_BACKOFF", 2*time.Minute),
		DeliveryTimeout:     envDurationOr("DELIVERY_TIMEOUT", 10*time.Second),
		LeaseDuration:       envDurationOr("LEASE_DURATION", 30*time.Second),
		PollInterval:        envDurationOr("POLL_INTERVAL", 500*time.Millisecond),
		BatchSize:           envIntOr("BATCH_SIZE", 8),
	}

	raw := envOr("ALLOWED_DESTINATIONS", "http://sink:9090/webhook,http://localhost:9090/webhook,http://127.0.0.1:9090/webhook")
	for _, d := range strings.Split(raw, ",") {
		d = strings.TrimSpace(d)
		if d != "" {
			c.AllowedDestinations = append(c.AllowedDestinations, d)
		}
	}
	if len(c.AllowedDestinations) == 0 {
		return nil, fmt.Errorf("ALLOWED_DESTINATIONS must list at least one URL")
	}
	if c.HMACSecret == "" {
		return nil, fmt.Errorf("HMAC_SECRET must be set")
	}
	return c, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envIntOr(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDurationOr(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
