# durable-webhook-delivery

Go service demonstrating **durable webhook delivery** with a Postgres transactional enqueue, lease-based workers, retries, HMAC-signed HTTP callbacks, dead-letter handling, and Prometheus metrics. It is a focused portfolio piece aimed at backend infrastructure patterns common around database-backed eventing and reliable outbound HTTP (complementary to wallet/ledger or SQL-proxy style projects).

**There is no affiliation with Shopee, TikTok, or any employer.** This repo is an independent demo for learning and interviews.

## What it does

| Component | Role |
|-----------|------|
| **API** (`cmd/api`) | `POST /v1/webhooks` (validated JSON + `Idempotency-Key`), `GET /v1/webhooks/{id}` |
| **Worker** (`cmd/worker`) | Claims pending work with `FOR UPDATE SKIP LOCKED`, sends HTTP POST with HMAC headers |
| **Sink** (`cmd/sink`) | Demo receiver that **verifies HMAC** and exposes `GET /received/{event_id}` for duplicate observation |
| **Postgres** | Source of truth: webhooks, delivery attempts, outbox rows, dead letters |

## Quick start (Docker Compose)

```bash
docker compose up --build
```

In another terminal:

```bash
# Enqueue
curl -sS -X POST http://localhost:8080/v1/webhooks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{
    "destination_url": "http://localhost:9090/webhook",
    "payload": {"event_id": "demo-1", "type": "order.paid"}
  }'

# Status (use id from response)
curl -sS http://localhost:8080/v1/webhooks/<uuid>

# Metrics
curl -sS http://localhost:8080/metrics | grep webhook_

# Sink receive count (dedupe observation)
curl -sS http://localhost:9090/received/demo-1
```

Repeat the same `POST` with the same `Idempotency-Key` and body → **200** with `"created": false` (no duplicate row).

## Local development (without Docker app images)

Requires Go 1.22+ and Postgres 16+.

```bash
export DATABASE_URL='postgres://webhook:webhook@localhost:5432/webhooks?sslmode=disable'
export HMAC_SECRET='dev-hmac-secret-change-me'
export ALLOWED_DESTINATIONS='http://localhost:9090/webhook'

# Terminal 1
go run ./cmd/sink

# Terminal 2
go run ./cmd/api

# Terminal 3
go run ./cmd/worker
```

### Tests

```bash
export DATABASE_URL='postgres://webhook:webhook@localhost:5432/webhooks?sslmode=disable'
go test -race -count=1 ./...
```

CI runs the same integration tests against a Postgres service container (see `.github/workflows/ci.yml`).

## Architecture

```mermaid
flowchart LR
  Client -->|POST /v1/webhooks| API
  API -->|txn: webhook + outbox| PG[(Postgres)]
  Worker -->|SKIP LOCKED claim| PG
  Worker -->|HMAC POST| Sink[Demo sink]
  Worker -->|finish attempt / DLQ| PG
  Client -->|GET status| API
```

**Enqueue path:** API validates payload JSON, checks `destination_url` against `ALLOWED_DESTINATIONS`, then inserts `webhooks` and `outbox_events` in one transaction.

**Delivery path:** Worker polls, recovers expired leases, claims rows with a time-bounded lease, POSTs to the sink with `X-Webhook-Timestamp` and `X-Webhook-Signature` (HMAC-SHA256 over `timestamp + "." + body`). Redirects are disabled (`ErrUseLastResponse`) so a 302 cannot bypass the allowlist.

## Design decisions

1. **Transactional outbox (lightweight):** Outbox rows are written with the webhook insert so enqueue is durable before any HTTP work. The worker marks outbox processed after a successful delivery (demo-level coupling; production might publish from outbox to a bus).

2. **Idempotency at enqueue only:** `Idempotency-Key` prevents duplicate **enqueue** when destination + JSON payload match. It does **not** imply exactly-once **delivery**.

3. **At-least-once delivery:** If the worker crashes **after** the sink returns 2xx but **before** `FinishAttempt` commits, another attempt may deliver again. Receivers must dedupe (e.g. on `event_id` in the payload). The demo sink counts deliveries per `event_id`.

4. **Leases + `SKIP LOCKED`:** Concurrent workers claim different rows; expired leases are recycled via `RecoverExpiredLeases`.

5. **SSRF guard:** Only explicit `ALLOWED_DESTINATIONS` URLs (normalized scheme/host/port/path) are accepted at enqueue and re-checked at send time. No arbitrary URLs; no redirect following.

6. **Retries & dead letter:** Exponential backoff capped by `MAX_BACKOFF`; after `MAX_DELIVERY_ATTEMPTS`, status becomes `dead_letter`.

## Configuration (environment)

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATABASE_URL` | local compose URL | Postgres DSN |
| `ALLOWED_DESTINATIONS` | sink URLs | Comma-separated allowlist |
| `HMAC_SECRET` | dev placeholder | Shared secret with sink |
| `MAX_DELIVERY_ATTEMPTS` | 5 | Before dead letter |
| `DELIVERY_TIMEOUT` | 10s | Per HTTP attempt |
| `LEASE_DURATION` | 30s | Claim lease |
| `WORKER_ID` | worker-1 | Lease owner label |

## Failure experiments

1. **Worker stop mid-flight:** `docker compose stop worker`, enqueue, start worker → delivery completes after restart.
2. **Sink 500:** Send header `X-Test-Fail: 1` from a custom client → retries until dead letter (lower `MAX_DELIVERY_ATTEMPTS` in compose for faster demo).
3. **Duplicate delivery:** Kill worker after sink returns 200 (harder to time manually); or read code path in `internal/delivery/worker.go` — success is recorded only after HTTP returns; crash before DB finish can redeliver.
4. **Idempotency conflict:** Same key, different `payload` → **409 Conflict**.
5. **SSRF attempt:** `destination_url` not on allowlist → **400**.

## Limitations (truthful)

- Single-region, single Postgres; no multi-tenant isolation.
- Outbox is not consumed by a separate publisher process.
- No encrypted secret store; `HMAC_SECRET` is plain env.
- No rate limiting, auth on enqueue API, or payload PII redaction in logs.
- Metrics use the default Prometheus registry (fine for demo; pin a dedicated registry in production).
- Allowlist is exact URL match, not per-tenant dynamic subscriptions.

## AI assistance

Parts of this repository were drafted with generic AI coding assistance during development: structure, SQL, tests, and documentation were iterated in-editor. **The author has not asserted production sign-off on this demo.** Review behavior via tests and source before reuse.

## License

MIT — see [LICENSE](LICENSE).
