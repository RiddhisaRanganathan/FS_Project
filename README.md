A runnable reference implementation of `FLASH_SALE_ARCHITECTURE.md`: 500 phones, multi-unit checkout, durable PostgreSQL admission, a transactional outbox, Kafka delivery, bounded FIFO allocation, and simulated payments. Accepted requests are distinct from confirmed purchases.

Python replaces the plan's suggested Go implementation. The consistency model, database, broker, and asynchronous workflow remain as specified. Redis inventory permits are deliberately omitted under C3's optional-layer provision; Redis provides atomic rate limiting and a versioned status cache. See [implementation decisions](docs/DECISIONS.md) for the review and tradeoffs.

## Start with Docker Compose

Install Docker Engine/Desktop with Compose v2. Allow roughly 4 GB of memory for the local stack. The containers use Python 3.12; native development should also use Python 3.12. This development stack binds plaintext services to localhost's published ports and uses demonstration database credentials. Use it on a trusted development machine.

1. Copy `.env.example` to `.env`.
2. Replace `AUTH_SECRET` and `ADMIN_SECRET` with distinct random secrets of at least 32 characters. For example: `python -c "import secrets; print(secrets.token_urlsafe(48))"`. Keep these values private.
3. Run:

```sh
docker compose up --build -d
docker compose logs -f migrate api worker
```

The `migrate` service waits for PostgreSQL, applies versioned migrations in a transaction under an advisory lock, and exits. API and worker start after migration success. The worker waits for Kafka and creates the two topics. Existing volumes survive ordinary restarts. Re-running migration is safe:

```sh
docker compose run --rm migrate
docker compose exec api python -m app token --buyer customer-001
```

Open **http://localhost:8000**, paste the printed customer token, select a quantity, and join the queue. Once reserved, select successful or declined simulated payment. No real money moves. Tokens expire after 24 hours; issue a new token with the same buyer identity to recover that buyer's attempts. Creating separate orders is allowed.

```sh
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
docker compose exec worker python -m app reconcile
```

`live` is process liveness. `ready` requires the database schema and Redis admission limiter; Kafka downtime is buffered in the durable outbox. Worker logs report broker failures and reconciliation every five seconds. Readiness does not claim Kafka is healthy or the queue is draining.

To stop, use `docker compose down`. Do not remove named volumes if accepted attempts must be preserved. Stock is initialized only by the initial migration; restarting does not refill stock.

## Native development

Create a Python 3.12 virtual environment and install `requirements.txt`. Start the dependencies with `docker compose up -d postgres redis kafka`. Export the variables in `.env.example` into the shell; native Python deliberately does not automatically load `.env` (Compose does). Set the same two secrets as the Compose services. Then run each long-running command in a separate terminal:

```sh
python -m venv .venv
# POSIX: . .venv/bin/activate
# PowerShell: .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m app migrate
python -m app api
python -m app worker
```

PowerShell environment syntax: `$env:AUTH_SECRET = 'your-secret'`. POSIX syntax: `export AUTH_SECRET='your-secret'`. The default database, Redis, and broker addresses target the published local ports. The native worker needs external Kafka listener `localhost:9092`; containers use `kafka:19092`.

## Tests

Tests use real PostgreSQL transactions. Each database test creates its own random schema and drops only that schema during cleanup. Use an isolated development/test database with schema-creation permission. Kafka tests use a unique consumer group and test attempt IDs; they add events to the local topics. Never point the test settings at production.

Run all tests against the Compose dependencies:

```sh
docker compose run --rm \
  -e TEST_DATABASE_URL=postgresql://sale:sale@postgres:5432/sale \
  -e TEST_REDIS_URL=redis://redis:6379/1 \
  -e TEST_KAFKA_BOOTSTRAP=kafka:19092 \
  api python -m pytest -q
```

On PowerShell, enter that command on one line instead of using POSIX backslashes. For native tests, set `TEST_DATABASE_URL`, `TEST_REDIS_URL`, and `TEST_KAFKA_BOOTSTRAP` to the corresponding localhost addresses and run `python -m pytest -q`. Unset dependency settings cause explicit skips, not simulated passes.

Authentication and static assets have checks that need only Python:

```sh
python -m unittest discover -s tests -p test_auth.py -v
python scripts/check_assets.py
python -m compileall -q app scripts tests
```

Exercise the running stack, including the actual Kafka consumer, scheduler, and payment timer:

```sh
docker compose exec api python scripts/smoke.py --url http://api:8000 --quantity 3
```

This consumes three units on success. Run it against a fresh development sale with stock available. Integration tests cover concurrent multi-unit allocation beyond 500 units, duplicate admission/messages/callbacks, payload conflicts, transaction rollback, FIFO delivery ordering, sold-out outcomes, release and reuse, late-payment refunds, expiry races, overload, status authorization, strict input validation, Redis outage, and publish-before-marker crash redelivery. They are separate from the running-stack smoke test.

Actual results and unexecuted checks are recorded in [docs/TEST_RESULTS.md](docs/TEST_RESULTS.md). No 100,000-request throughput result has been established.

## Load test

Set `AUTH_SECRET` in the load-generator shell to the local API's secret. This is a trusted local test tool that signs one customer identity per request; do not distribute production signing secrets to load clients.

```sh
python scripts/load.py --requests 100000 --duration 10 --concurrency 256 --queue-capacity 20000
```

The generator schedules arrivals independently of response time and bounds its client queue. Results include client drops, scheduled-to-send lag, HTTP latency p50/p95/p99, response codes, drain time, and uncertain request keys. High scheduling lag or client drops invalidate any claim that the intended arrival rate was delivered. Increase generators/resources only after measuring the bottleneck. `--quantity`, `--url`, and `--output` are configurable. The default output is `test-results/load.json`.

This script measures admission, not completion, projection throughput, or mobile performance. Compare results with operator reports, worker logs, PostgreSQL lock/connection metrics, outbox drain time, and Kafka lag. Run status polling as a separate workload at its planned concurrency. Do not infer 10,000 durable writes/second from the architecture or from client dispatch counts.

## Checkout lifecycle

1. The browser persists a cryptographic attempt key and quantity before sending. An interrupted submission keeps the same key across refreshes. Recovery first looks up the key; a missing attempt can be submitted again with that key.
2. The API validates the signed customer, sale/SKU, integer quantity 1–500, request size, account rate, and pending capacity. Admission and its outbox event commit in one PostgreSQL transaction. Only then is `202` returned. Reusing a key with different details returns `409`. Replaying an existing attempt returns its current status.
3. The publisher sends to Kafka with idempotence and `acks=all`, then marks the outbox row. An uncertain publication is retried; duplicates are expected. The consumer atomically records the event inbox entry and durable dispatch flag before committing its offset.
4. A sale scheduler chooses the smallest durable sequence. The oldest request waits for its broker delivery and for enough stock. One conditional inventory update allocates its entire quantity, creates its reservation, and changes the attempt to `RESERVED`, in one transaction.
5. Payment creates a durable simulated provider record using the order's stable identity. The timed callback reads that record; caller-supplied webhook data cannot invent a success. Payment and expiry both lock the attempt and reservation in the same order.
6. Timely success consumes the active reservation, transfers reserved units to sold, confirms the order, records payment success, and inserts the fulfillment outbox event atomically. Fulfillment starts through Kafka and inserts an idempotent fulfillment record. This record represents simulated dispatch, not a physical shipment integration.
7. Decline or expiry releases units exactly once. A late successful payment cannot revive the order: it becomes a durable refund task and then a simulated refund. Waiting requests may use the released units.
8. The UI polls with jitter and backoff, refreshes when the tab becomes visible, and displays server-timed expiry. A version-aware Redis cache bounds repeated status reads. Redis loss permits only a bounded number of authoritative fallbacks; it never fabricates purchase success.

## API

All checkout/status/payment endpoints require `Authorization: Bearer <customer-token>` and enforce buyer ownership.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/product` | Static catalog and price |
| POST | `/api/checkout-attempts` | Durable admission; `Idempotency-Key` required |
| GET | `/api/checkout-attempts/by-key/{key}` | Recover uncertain admission |
| GET | `/api/checkout-attempts/{id}` | Versioned status; ETag / `If-None-Match` support |
| POST | `/api/checkout-attempts/{id}/payment` | Simulated provider; `{outcome:"success" or "failure",delaySeconds:0..900}` |
| POST | `/api/simulator/callback/{id}` | Operator-triggered delivery of an already-due provider result |
| GET | `/api/ops/report` | Inventory/order conservation, queue, outbox, refund counts |
| GET | `/health/live`, `/health/ready` | Process and dependency checks |

Simulator callbacks and operator reports require `X-Admin-Secret`. Payment fields are simulation controls, not a model for trusting real payment results. OpenAPI is available at `/docs`.

## Operations and configuration

`MAX_PENDING` caps pending attempts and unpublished outbox rows at admission. `MAX_INFLIGHT` bounds HTTP work; `DB_POOL_SIZE` bounds each process's pool. The worker has one consumer, one publisher, and one scheduler, with bounded batches and a 1 MB consumer prefetch limit. Additional worker processes share PostgreSQL scheduler fencing. Scaling pools/processes requires budgeting their aggregate database connections.

`ADMISSION_PAUSED`, `RESERVATIONS_PAUSED`, and `RECONCILIATION_ONLY` are environment switches. Change `.env` and recreate the affected services with `docker compose up -d --force-recreate api worker`. Pausing reservations leaves expiry/payment maintenance active. Reconciliation-only stops new allocation and automated processing for investigation; expiry catches up on normal restart. Unhealthy reconciliation stops the scheduler and emits an error-bearing report; it never rewrites counters to hide discrepancies.

Transient outbox/consumer failures use capped exponential backoff with jitter. Infrastructure errors stay retriable indefinitely, with capped delay, while admission is bounded. Invalid envelopes are recorded in `dead_letters` and sent to `sale.dead-letter` before their source offset is committed. Investigate worker logs and retained ledger records, correct the cause, then replay a canonical outbox event:

```sh
docker compose exec worker python -m app replay --event-id 123
```

Replay keeps the original event identity and business guards. It does not delete inbox records or allocate again. Do not blindly replay an external poison payload. Event records unprocessed after a 60-second publication grace are automatically republished from the ledger, including after broker retention loss. No accepted ledger/outbox records are automatically deleted in this reference project.

## Project structure

```text
app/                  API, auth, configuration, bounded database pool,
                      checkout transactions, Kafka worker, CLI
web/                  Responsive product page and recoverable checkout UI
migrations/           Versioned PostgreSQL schema and initial 500 units
tests/                Authentication, inventory/race, HTTP, Kafka integration tests
scripts/              Asset checks, running-stack smoke test, open-loop load test
docs/                 Decisions, failure analysis, validation and actual test results
compose.yaml          PostgreSQL, Redis, Kafka, migration, API and worker services
Dockerfile            Python 3.12 application image
.env.example          Local configuration template
requirements.txt      Pinned runtime/test dependencies
FLASH_SALE_ARCHITECTURE.md  Original plan plus implementation addendum
```

See [failure analysis and requirement checklist](docs/VALIDATION.md) for scope, known limits, and production work.
