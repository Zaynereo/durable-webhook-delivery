# Interview notes — durable-webhook-delivery

## Architecture (concise)

- **API:** HTTP validation, allowlist check, idempotent enqueue in a DB transaction (`webhooks` + `outbox_events`).
- **Store:** Postgres with `delivery_attempts` leases, `webhook_dead_letters`, JSONB payloads.
- **Worker:** Poll → recover expired leases → `FOR UPDATE SKIP LOCKED` claim → HMAC POST → finish attempt or schedule backoff.
- **Sink:** Verifies HMAC; counts `event_id` for duplicate observation.

## Key code paths

| Path | Package / file |
|------|----------------|
| Enqueue txn | `internal/store/store.go` — `Enqueue` |
| Claim + lease | `internal/store/store.go` — `ClaimPending`, `RecoverExpiredLeases` |
| HTTP delivery | `internal/delivery/worker.go` — `deliverOne` |
| Allowlist / SSRF | `internal/destination/allowlist.go` |
| HMAC | `internal/hmacsig/signer.go` |
| HTTP API | `internal/api/handler.go` |

## Tradeoffs

- **Outbox co-located with worker success** — simpler demo, not a standalone relay service.
- **JSONB equality for idempotency** — avoids whitespace false conflicts; still requires identical semantic JSON.
- **Lease expiry recovery** — marks attempt finished with retry; can add jitter in production.
- **At-least-once** — chosen over distributed exactly-once (no Kafka transactions, no sink 2PC).

## Known gaps

- No API authentication, no per-destination signing secrets, no circuit breaker per sink.
- No partitioning / archival of `delivery_attempts`.
- No OpenTelemetry traces (metrics only).

---

## Interview Q&A (30)

1. **Q:** What problem does this service solve?  
   **A:** Reliable asynchronous HTTP notification after an event is accepted, surviving process restarts and transient downstream failures.

2. **Q:** Why Postgres instead of a queue only?  
   **A:** Enqueue and business state share one durable transaction; operations teams already run Postgres; `SKIP LOCKED` gives worker scalability without Redis for this demo.

3. **Q:** What is the transactional outbox pattern here?  
   **A:** Insert webhook + outbox row in one commit so we never acknowledge enqueue without durable storage.

4. **Q:** Does idempotency guarantee exactly-once delivery?  
   **A:** No. It dedupes **enqueue** only. Delivery is at-least-once; sinks dedupe on business keys.

5. **Q:** When can duplicates reach the sink?  
   **A:** After successful HTTP response if the worker crashes before `FinishAttempt`, or on lease recovery while HTTP is still in flight (mitigated by timeouts).

6. **Q:** How do you prevent SSRF?  
   **A:** Explicit `ALLOWED_DESTINATIONS`; normalize URLs; disable HTTP redirects; re-validate at send time.

7. **Q:** Why disable redirects?  
   **A:** A allowed URL could redirect to an internal or attacker URL; `CheckRedirect` returns `ErrUseLastResponse`.

8. **Q:** Explain `FOR UPDATE SKIP LOCKED`.  
   **A:** Workers lock candidate webhook rows; others skip locked rows and claim different work without blocking.

9. **Q:** What is the lease for?  
   **A:** If a worker dies, `lease_expires_at` passes, recovery marks the attempt failed/retryable so another worker can claim.

10. **Q:** How is backoff computed?  
    **A:** Exponential from `INITIAL_BACKOFF`, capped at `MAX_BACKOFF` in `store.Backoff`.

11. **Q:** When does dead letter happen?  
    **A:** After `MAX_DELIVERY_ATTEMPTS` failed attempts, row moves to `webhook_dead_letters` and status `dead_letter`.

12. **Q:** How is HMAC constructed?  
    **A:** `HMAC-SHA256(secret, timestamp + "." + body)` hex-encoded in `X-Webhook-Signature`.

13. **Q:** Why include a timestamp in the signature?  
    **A:** Allows receivers to reject stale replays (sink demo verifies crypto only; production adds skew window).

14. **Q:** What HTTP status on duplicate idempotent enqueue?  
    **A:** 200 with `created: false` when key, destination, and payload match; 409 on conflict.

15. **Q:** How would you scale workers?  
    **A:** Horizontally run more `cmd/worker` processes; DB is coordination via leases and `SKIP LOCKED`.

16. **Q:** Bottleneck at high throughput?  
    **A:** Postgres row-level contention on hot queues; would shard by tenant, partition attempts, or externalize queue.

17. **Q:** How do you test concurrent claims?  
    **A:** Integration test enqueues many webhooks, parallel `ClaimPending`, assert no duplicate webhook IDs claimed.

18. **Q:** How do you test lease recovery?  
    **A:** Short lease, sleep past expiry, `RecoverExpiredLeases`, claim again.

19. **Q:** Difference from CDC / Debezium?  
    **A:** CDC streams DB changes; this is **intent to call an HTTP endpoint** stored as application-level work.

20. **Q:** Relation to DBaaS work?  
    **A:** Shows comfort with transactions, indexing, operational recovery — adjacent to replication/proxy tooling without implementing MySQL protocol.

21. **Q:** Relation to live-streaming / payments?  
    **A:** Both domains need reliable side effects (notify treasury, fan-out events); pattern is generic outbound webhooks.

22. **Q:** What metrics exist?  
    **A:** `webhook_enqueue_total`, `webhook_delivery_attempts_total`, `webhook_dead_letters_total`, `webhook_deliveries_in_flight`.

23. **Q:** What would you add for production?  
    **A:** mTLS or signed enqueue tokens, per-destination secrets, DLQ replay UI, SLO dashboards, payload size quotas.

24. **Q:** Why not use SQS/RabbitMQ?  
    **A:** Valid choice; Postgres fits portfolio scope and demonstrates SQL concurrency primitives.

25. **Q:** How to replay dead letters?  
    **A:** Not implemented; would reset status, clear DLQ row, set `next_retry_at` in a controlled admin API.

26. **Q:** Exactly-once options?  
    **A:** Idempotent sink + dedupe store, or transactional outbox with Kafka and consumer offsets — heavier infra.

27. **Q:** JSONB for idempotency compare?  
    **A:** SQL `payload = $3::jsonb` compares semantic JSON, not raw string whitespace.

28. **Q:** Why separate API and worker binaries?  
    **A:** Independent scaling and deploy; API stays fast while workers do I/O-bound HTTP.

29. **Q:** What fails if `ALLOWED_DESTINATIONS` misconfigured?  
    **A:** Legitimate traffic rejected at enqueue; misconfiguration is safer than open SSRF.

30. **Q:** How does CI test Postgres?  
    **A:** GitHub Actions service container; `go test -race` on store, api, delivery packages with `DATABASE_URL`.
