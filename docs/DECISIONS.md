# Plan review and implementation decisions

The existing architecture is preserved. These decisions close its implementation gaps; they do not establish its proposed capacity claims.

| Gap or risk in the plan | Implemented decision |
| --- | --- |
| Go is suggested, with no existing source or toolchain | Python 3.12, FastAPI, psycopg, confluent-kafka; language changes no transaction boundary. The available workspace has only Python. |
| Redis permit TTL does not restore its counter; ownership and rebuild fencing unspecified | Use C3's explicit option to omit provisional inventory permits. No permit counter, token expiry, ownership release, or failover rebuild can influence allocation. Redis Lua is used only where atomicity is sufficient: rate limits and monotonically versioned cached status. |
| Fairness policy needs business approval | Development policy is FIFO by admission sequence under the admission transaction lock. No waiting-room priority, randomized lottery, or per-customer unit limit is implied. Production fairness requires approval. |
| Kafka partition order can conflict with FIFO | Kafka records durable dispatch eligibility; a PostgreSQL-fenced scheduler selects the oldest queued attempt, waiting if its delivery is delayed. |
| Fully reserved stock may become available again | Keep remaining attempts queued while any active reservation exists. If the head request cannot fit after all reservations settle, mark it sold out and advance. This deliberately permits head-of-line blocking; no partial allocations or priority skipping. |
| Durable queue needs a capacity boundary | Serialize admission capacity checks with an independent advisory lock. Bound pending attempts and unpublished events. Return 503 before a new acceptance at capacity. Idempotent existing requests are looked up first inside this transaction. This simple global gate needs benchmarking and is not claimed to handle the target burst. |
| Payment success and expiry may race | Both lock attempt then reservation; check database clock after acquiring attempt ownership. Timely success consumes; expiry releases. A callback received after the deadline cannot confirm even if the simulator's payment was scheduled earlier. Late success creates a refund task. |
| Payment/provider/order state unspecified | One attempt is one logical order, with separate reservation, payment, inbox, and fulfillment tables. Payment provider is a durable simulator table keyed uniquely by attempt/provider ID. Real provider integration is out of scope. |
| Externally visible intermediate states | Reservation is a short transaction: QUEUED becomes RESERVED atomically. RESERVING is not persisted because there is no durable work between them. PAYMENT_PENDING lasts until the simulated result is applied. FAILED includes expiry/decline with a reason. |
| Exactly-once effect versus broker delivery | Unique inbox events, one reservation per attempt, locked terminal transitions, and unique fulfillment records. Kafka is at least once; producer idempotence is supplemental. |
| Consumer retries may exhaust broker retention | PostgreSQL retains canonical events and inbox/dispatch/fulfillment outcomes. Reconciliation republishes unprocessed events older than 60 seconds; duplicate delivery remains safe. Retention still matters for other subscribers. |
| Poison messages versus dependency failure | Invalid envelopes/source mismatches go to durable dead-letter storage and topic. Infrastructure errors retry indefinitely with capped delay; they never silently discard accepted orders. This can block a consumer partition and requires alerting. |
| Status projection ordering | Redis compare-and-set uses version numbers; cache lifetime is two seconds. Reads are fenced by buyer identity. Missing Redis/cache entries use four bounded concurrent database fallbacks per API process. Stale terminal confirmations cannot be created by this projection. |
| Operational controls and reconciliation | Environment pause switches applied by service recreation; five-second snapshot reconciliation checks conservation, reservation totals, confirmed totals, and order/reservation state/quantity relationships. No automatic correction of authoritative inventory. |
| Simulation of fulfillment | A confirmed Kafka event inserts a unique fulfillment record. No real shipment or external side effect is claimed. |

## Transaction boundaries and locks

Admission commits the attempt and `admitted` outbox event together; acceptance is sent after the connection context commits. A duplicate admission uses the same customer/key uniqueness constraint and compares sale, SKU, and quantity. Status lookup after admission can fail despite a committed attempt; the resulting uncertain HTTP outcome is recovered with the same key.

Allocation holds the per-sale scheduler advisory lock, then the attempt row, then performs the atomic `available >= quantity` inventory update. It creates the reservation and advances the order before commit. Payment start, payment completion, and expiry lock the attempt first, then the reservation, then update inventory as necessary. Payment completion also locks its provider row. No inventory transaction calls Redis, Kafka, or an external provider.

The outbox publisher intentionally holds an outbox-row lock across broker acknowledgment, isolated from inventory allocation; crashes roll back its publication marker. Consumers commit PostgreSQL before Kafka offsets. The admission lock and scheduler lock have different IDs; publication never locks inventory. Pools and connection/statement/lock timeouts bound resource waits.

The 500-unit constraint is hard-coded in the database for this one sale, with nonnegative available/reserved/sold and their sum equal to 500. Direct arbitrary database writes or a warehouse allocating the same physical stock are outside the application guarantee. Physical inventory must be exclusively assigned to this sale.

References used to check client transaction/delivery behavior: [psycopg connection transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html), [Confluent Python client manual commits](https://docs.confluent.io/kafka-clients/python/2.15/overview.html).
