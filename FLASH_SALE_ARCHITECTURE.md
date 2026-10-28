The design assumes a flash sale involving 500 smartphones of a single SKU, with more than 100,000 checkout requests arriving within 10 seconds. Customers may purchase multiple units of the same SKU in one order, subject to stock availability. PostgreSQL remains the authoritative inventory database, and the system must prevent overselling while preserving durably accepted checkout attempts.

> Implementation addendum (2026-10-28): The application now lives in `app/`, `web/`, and `migrations/`, with Compose infrastructure and tests. Read `docs/DECISIONS.md` for the bounded FIFO policy, Python implementation choice, explicit omission of optional Redis inventory permits, payment/expiry lock protocol, and broker-retention recovery. Redis remains a rate-limit and versioned-status layer. `docs/VALIDATION.md` distinguishes local implementation from production deployment work; `docs/TEST_RESULTS.md` records actual executed and blocked checks. The throughput and browser-performance targets below remain unverified targets. The original plan is preserved below.

# Phase A: Frontend Architecture

## A1. Lightweight and Non-Blocking Checkout Design

The frontend will be designed as a lightweight, responsive application capable of supporting a large number of users during the flash sale. The checkout process will follow an asynchronous approach in which the client submits a checkout request and receives an acknowledgment once the request has been durably accepted. Inventory allocation, order processing, and payment verification will be performed separately by backend services. This prevents users from waiting for the entire checkout workflow to complete within a single HTTP request.

The application will target an initial JavaScript bundle size of less than 200 KB, measured as the combined minified and gzip-compressed JavaScript required for the checkout route, including the framework, runtime, and essential application code. The Time to Interactive target will be less than 1.5 seconds under a defined test environment using a representative mid-range mobile device, throttled 4G connectivity, and a cold browser cache. These are proposed performance targets and must be validated through controlled testing.

To achieve these targets, static resources such as JavaScript, CSS, fonts, and product images will be distributed through a Content Delivery Network (CDN). Code splitting and lazy loading will ensure that nonessential features, including recommendations, reviews, and post-purchase interfaces, do not delay the checkout page. Prefetching may be used to load likely navigation resources before the sale begins, while pre-rendering can make the product page and countdown visible before the complete JavaScript application initializes. Prefetching must not initiate an actual checkout or reserve inventory.

The frontend will also minimize unnecessary network requests and avoid blocking the main browser thread. Performance will be monitored using metrics such as Time to Interactive, Largest Contentful Paint (LCP), Interaction to Next Paint (INP), and long tasks. This approach improves responsiveness without requiring the frontend to perform inventory or payment operations synchronously.

## A2. Duplicate Submission Prevention and Idempotency

During a high-demand flash sale, users may repeatedly click the checkout button because of slow responses, network interruptions, or uncertainty about whether their request was submitted. Such behavior can generate duplicate requests and increase backend load. The frontend will therefore implement debouncing or throttling and disable the checkout button immediately after submission.

Each logical checkout attempt will be assigned a cryptographically random idempotency key. The same key will be reused when retrying that attempt, including after a page refresh or network timeout. The backend will associate the key with the authenticated customer, sale identifier, and canonical request details, including the requested quantity. If the same request is received again, the backend will return the existing checkout attempt rather than creating another one. Reusing a key with different request details will result in an HTTP `409 Conflict` response.

For example, the following request allows a customer to purchase multiple units of the same SKU:

```
async function submitCheckout(quantity) {
  if (checkoutState !== "ready") return;

  checkoutState = "submitting";
  renderCheckoutState();

  const idempotencyKey = getOrCreateAttemptKey();

  try {
    const response = await fetch("/api/checkout-attempts", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey
      },
      body: JSON.stringify({
        saleId: "festival-sale",
        sku: "phone-001",
        quantity: quantity
      }),
      signal: AbortSignal.timeout(8000)
    });

    await handleAdmissionResponse(response);
  } catch (error) {
    checkoutState = "retrying";
    showStatusRecoveryOption();
  }
}
```

The eight-second timeout is an illustrative configuration rather than a validated optimal value. A timeout does not necessarily mean the server rejected the request; the request may already have been committed while its response was lost. Therefore, the client must retrieve the existing attempt using its stable identifier or idempotency key before creating another checkout attempt.

A customer may purchase multiple phones within an order. Whether customers may create additional independent orders is a separate business rule and must not be confused with idempotency, which prevents duplicate processing of the same logical request.

## A3. Checkout Status Management and User Experience

The checkout interface will use a state-based workflow to communicate the progress of each request. Initially, the customer can select the desired quantity and submit the order. The interface then enters a submitting state and displays immediate feedback. Once the backend has durably accepted the request, the frontend displays a queued status rather than indicating that the purchase has succeeded.

When the backend begins inventory allocation, the status changes to reserving. If the full requested quantity is available and successfully reserved in PostgreSQL, the customer is informed that the inventory is temporarily held and can proceed with payment. The reservation countdown will be based on a server-provided expiration timestamp rather than relying solely on the customer's device clock.

The order will be displayed as confirmed only after the payment provider's successful result has been verified and the corresponding database transaction has committed. If insufficient inventory is available, the system will communicate the outcome clearly. Under the proposed all-or-nothing allocation policy, an order requesting five phones will not automatically receive three if only three remain. Partial fulfillment would require a separate, explicitly approved business workflow.

Optimistic UI updates will be limited to reversible interactions such as disabling the checkout button, displaying progress indicators, and showing temporary submission feedback. They will not be used to display successful purchases or inventory reservations before authoritative confirmation. If a request fails, the interface will show the actual reason and permit a retry only when it is safe to do so.

Accessibility will be incorporated into the checkout workflow through keyboard navigation, visible focus indicators, sufficient contrast, reduced-motion support, and screen-reader announcements. Meaningful status changes such as queued, reserved, failed, and confirmed will be communicated using appropriate ARIA live regions. When a browser tab becomes active after being backgrounded, the frontend will retrieve the latest server status rather than trusting potentially outdated local state.

## A4. Checkout Status Updates: Polling, SSE, and WebSockets

Because checkout processing is asynchronous, the frontend requires a mechanism to retrieve status updates after the initial request has been accepted. Polling, Server-Sent Events (SSE), and WebSockets are possible approaches.

| Mechanism          | Advantages                                                         | Limitations                                                     |
| ------------------ | ------------------------------------------------------------------ | --------------------------------------------------------------- |
| Polling            | Simple implementation, stateless requests, easy horizontal scaling | Repeated HTTP requests and delayed updates                      |
| Server-Sent Events | Efficient one-way updates without repeated polling                 | Requires management of long-lived connections and reconnections |
| WebSockets         | Low-latency bidirectional communication                            | Higher connection-management and infrastructure complexity      |

Adaptive polling is recommended for this architecture because checkout status updates are relatively infrequent and do not require continuous bidirectional communication. The frontend may initially poll every two seconds and gradually increase the interval to five or ten seconds for customers waiting in a long queue. These intervals are starting values and must be adjusted through performance testing.

Randomized polling intervals, also known as jitter, will prevent thousands of clients from sending requests at exactly the same time. The backend may also provide a recommended next polling interval through a response field or the `Retry-After` header. Conditional requests using ETags can reduce unnecessary response payloads.

Polling traffic must be capacity-planned independently from checkout submissions. For example, if 100,000 customers poll every five seconds, the system receives approximately 20,000 status requests per second on average. Therefore, status responses will be served from a scalable, read-optimized projection rather than repeatedly executing expensive queries against the primary inventory database.

Overall, the frontend architecture prioritizes responsiveness, clear status communication, and safe recovery from uncertain network outcomes. Adaptive polling introduces a small delay in displaying updates but reduces the operational complexity associated with maintaining large numbers of persistent connections.

# Phase B: Gateway, Admission, Queues, and Workers

## B1. Request Handling and Load Balancing

The system must accommodate more than 100,000 checkout requests arriving within the first ten seconds of the flash sale. If exactly 100,000 requests arrive evenly over ten seconds, the average arrival rate is:

\\[ \lambda\_{\text{avg}}=\frac{100,000}{10}=10,000\text{ requests/second} \\]

However, the actual peak may be considerably higher because users are likely to submit requests immediately when the sale opens. Infrastructure capacity must therefore be based on measured peak traffic rather than the ten-second average alone.

Incoming requests will first pass through a CDN, Web Application Firewall (WAF), and highly available Layer 7 load balancer. The load balancer will distribute requests across stateless API replicas, using a routing strategy such as least-outstanding-requests where supported and validated. Because session state and checkout attempts are stored in shared durable systems, sticky sessions will not be required.

The gateway will enforce request-size limits, schema validation, authentication, rate limits, connection limits, and short request-header timeouts. Rate limiting will consider multiple signals, including IP address, authenticated account, device characteristics, and sale-specific activity. Relying only on IP addresses would risk incorrectly blocking legitimate customers sharing mobile carrier networks or household connections.

Bot detection and challenge mechanisms will help reduce automated abuse. However, these controls must be designed carefully because aggressive filtering can exclude legitimate users. When downstream services approach their safe operating limits, the gateway will apply backpressure or reject new requests rather than allowing unbounded in-memory queues and connection growth.

For fair access, a pre-sale waiting room can issue authenticated, signed admission tickets. Customers present before the sale opens may be assigned randomized priority, while later arrivals can be handled according to a defined queue policy. This prevents the allocation process from favoring users who generate the most requests or have the lowest network latency. The precise fairness policy requires business approval and must be enforced by durable admission records rather than relying on frontend behavior.

## B2. Checkout Admission API and Durable Request Acceptance

The checkout API will be responsible for validating the customer, sale eligibility, requested SKU, quantity, and idempotency key before accepting a request. A successful admission does not mean that inventory has been allocated or payment has been completed. It means that the request has been recorded durably and can be recovered for asynchronous processing.

The API will use a PostgreSQL admission ledger and transactional outbox to establish this durability boundary. Within one database transaction, the service will create or retrieve the logical checkout attempt and insert the corresponding outbox event. Uniqueness constraints will prevent duplicate records for the same idempotency key. Once the transaction commits successfully under the configured durability policy, the API can return `202 Accepted`.

An example response is:

```
HTTP/1.1 202 Accepted
Content-Type: application/json
Location: /api/checkout-attempts/att_123
Retry-After: 3
```

```
{
  "attemptId": "att_123",
  "status": "QUEUED",
  "purchaseConfirmed": false,
  "statusUrl": "/api/checkout-attempts/att_123"
}
```

The transactional outbox is important because writing directly to Kafka and separately updating the database can create inconsistencies. For example, Kafka may accept a message while the database transaction fails, or the database may commit while Kafka is temporarily unavailable. By committing the admission record and outbox event together, the system ensures that accepted attempts can be published to Kafka later without losing their durable identity.

An outbox publisher will forward committed events to Kafka and mark them as published after successful acknowledgment. Since a publisher may crash after sending an event but before recording that it was sent, duplicate publication is possible. Consumers must therefore be idempotent.

If the admission database cannot commit a request, the API must not report it as accepted. Requests rejected because of overload may receive `503 Service Unavailable` with a retry hint, while requests blocked by rate limiting may receive `429 Too Many Requests`. Accepted attempts remain recoverable even if the API process crashes or the customer disconnects.

The admission ledger is itself a potential bottleneck because it may need to sustain the initial burst of writes. Its throughput, uniqueness checks, indexes, replication latency, and connection pools must be benchmarked independently. If the database cannot meet the required admission rate, the architecture must use bounded admission or a more scalable durable admission mechanism rather than weakening durability guarantees.

## B3. Message Queue Selection: Kafka vs. RabbitMQ

A message broker will decouple the initial burst of checkout requests from inventory reservation and payment processing. Instead of allowing every request to execute inventory transactions immediately, accepted attempts will be published to a durable queue and processed by a controlled number of workers.

Kafka and RabbitMQ are both suitable messaging technologies, but they have different strengths.

| Criteria                   | Apache Kafka                                               | RabbitMQ                                                                    |
| -------------------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------- |
| Messaging model            | Distributed, partitioned event log                         | Message queues with routing and acknowledgments                             |
| High-throughput processing | Strong fit for large event streams                         | Suitable for high-throughput task processing with appropriate configuration |
| Replay and recovery        | Supports replay through retained log records               | Supports redelivery and recovery through queue mechanisms                   |
| Message ordering           | Guaranteed within a partition                              | Depends on queue and consumer configuration                                 |
| Retry handling             | Typically uses retry topics or scheduling                  | Supports queue-based retry and dead-letter patterns                         |
| Operational complexity     | Requires partition, broker, and retention management       | Requires queue topology and broker management                               |
| Suitability                | Durable event processing and multiple downstream consumers | Task distribution and acknowledgment-based workflows                        |

Kafka is selected for this design because it supports durable event retention, replayable processing, consumer groups, and independent downstream services. It also allows admission events to be reused by status projection, monitoring, and other asynchronous workflows.

Kafka will be configured with replicated partitions, appropriate producer acknowledgment settings, producer idempotence, and suitable in-sync replica requirements. These settings improve message durability within the declared failure model but do not eliminate every possible infrastructure failure.

The system will assume at-least-once message delivery. Consequently, a message may be processed more than once after a consumer restart, network failure, or uncertain acknowledgment. Exactly-once inventory allocation will be achieved through PostgreSQL constraints and idempotent state transitions, not by assuming the message broker will never redeliver an event.

Failed messages will be retried using exponential backoff with jitter. Permanent failures and messages that repeatedly fail processing will be routed to a dead-letter topic for investigation and controlled replay. Kafka retention and queue backlog limits must be configured so that accepted attempts remain recoverable throughout the expected processing and recovery period.

## B4. Backend Worker Architecture and Concurrency Control

The backend will be divided into independently scalable services with clearly defined responsibilities. The admission API will validate and durably accept requests, while the outbox publisher will transfer accepted events to Kafka. Admission consumers will select eligible requests according to the fairness policy, and reservation workers will perform inventory allocation. Separate payment, status projection, reservation expiry, reconciliation, and fulfillment components will handle subsequent workflow stages.

Go is a suitable implementation language because its goroutines and context-based cancellation support concurrent network operations efficiently. However, concurrency must be bounded. Creating an unrestricted goroutine for every queued checkout attempt could exhaust memory, database connections, and downstream service capacity.

Workers will therefore use fixed concurrency limits, bounded in-flight message counts, database connection pools, and request deadlines. Worker scaling will consider Kafka consumer lag together with PostgreSQL lock waits, Redis latency, and payment-provider limits. Increasing worker concurrency when the database is already saturated would worsen contention rather than improve throughput.

The reservation worker will process each message using a stable attempt identifier. Before allocating stock, it will inspect the current order or attempt state. If the message represents an already completed reservation, the worker will return the existing result without modifying inventory again. A Kafka offset will be committed only after the corresponding durable database transition has completed or the worker has verified that the event was previously processed.

For checkout requests containing multiple units, the worker will pass the requested quantity to the inventory transaction. It will not create separate independent reservations for each phone unless the data model explicitly requires per-unit allocation.

## B5. API Status Management, Retries, and Recovery

The backend will expose a status endpoint such as `GET /api/checkout-attempts/{attemptId}`. This endpoint will return the latest checkout state, including queued, reserving, reserved, payment pending, confirmed, sold out, or failed. A successful status lookup will normally return HTTP `200 OK`, even when the business outcome is sold out or failed, because the HTTP response indicates that the lookup itself succeeded.

PostgreSQL will store the authoritative attempt and order state, while a read-optimized projection will serve high-volume polling requests. Each state transition will carry a monotonically increasing version so that delayed or duplicated events cannot overwrite newer status information. If the projection is unavailable, the API may perform a rate-limited authoritative lookup rather than returning an invented confirmation.

Transient errors will be retried with bounded exponential backoff and jitter. Permanent failures, such as invalid eligibility or insufficient inventory for a terminal request, will not be retried indefinitely. The system will distinguish between requests that were never durably accepted and accepted requests that are still awaiting processing.

If Kafka becomes unavailable, accepted requests will remain in the PostgreSQL outbox until publishing resumes. If a worker crashes before committing its transaction, the database will roll back the incomplete work and Kafka can redeliver the message. If a worker crashes after a successful commit but before acknowledging Kafka, the next processing attempt will identify the existing durable state and avoid duplicate allocation.

This architecture increases infrastructure complexity compared with a synchronous checkout API, but it isolates the initial traffic burst from inventory processing and provides a recoverable processing path for accepted requests.

# Phase C: Inventory Consistency and Database Design

## C1. Database Architecture and Inventory Consistency

The most critical requirement of the flash-sale system is to ensure that no more than 500 smartphones are allocated, regardless of how many customers submit checkout requests simultaneously. The previous system confirmed 540 purchases despite having only 500 units available, indicating that inventory allocation was not consistently enforced during concurrent transactions.

A common cause of overselling is the separation of inventory verification and inventory updates. For example, two transactions may both read that five phones are available and independently approve orders for four phones. If the transactions do not enforce the stock condition during the actual update, both orders may succeed even though their combined quantity exceeds the available inventory.

To eliminate this race condition, PostgreSQL will serve as the single authoritative source for inventory allocation. Redis may be used to reduce unnecessary database traffic, but it will not independently determine whether an order is confirmed or inventory has been allocated.

The database will maintain four inventory values: `total_stock`, `available`, `reserved`, and `sold`. For this sale, the initial stock is exactly 500 units. The inventory conservation rule is:

\\[ available + reserved + sold = 500 \\]

All three changing quantities must remain nonnegative. A phone moves from available to reserved when a reservation is successfully committed, and from reserved to sold after payment verification. If a reservation expires or is cancelled before confirmation, its quantity returns to available stock.

The following PostgreSQL schema enforces these constraints:

```
CREATE TABLE sale_inventory (
    sale_id UUID PRIMARY KEY,
    sku TEXT NOT NULL,
    total_stock INT NOT NULL CHECK (total_stock >= 0),
    available INT NOT NULL CHECK (available >= 0),
    reserved INT NOT NULL CHECK (reserved >= 0),
    sold INT NOT NULL CHECK (sold >= 0),

    CHECK (
        available + reserved + sold = total_stock
    )
);
```

The sale will be initialized with `total_stock = 500`, `available = 500`, `reserved = 0`, and `sold = 0`. These constraints provide database-level protection against negative stock and violations of the inventory conservation equation.

The 500 physical units must also be assigned exclusively to this sale. If another marketplace or warehouse system can independently allocate the same inventory, PostgreSQL cannot guarantee correctness without coordination with those external systems.

## C2. Atomic Inventory Reservation Using PostgreSQL

Inventory allocation will use an atomic conditional update rather than a separate stock-checking query followed by an unconditional update. The requested quantity will be deducted only when sufficient inventory remains.

```
UPDATE sale_inventory
SET available = available - $2,
    reserved = reserved + $2
WHERE sale_id = $1
  AND available >= $2
RETURNING available, reserved;
```

Here, `$1` represents the sale identifier and `$2` represents the number of smartphones requested by the customer.

For example, if seven phones remain and two customers simultaneously request five units each, only one request can successfully reserve five phones. The other request will fail the inventory condition after the first transaction updates the row. PostgreSQL serializes conflicting updates to the same inventory record, and under its normal `READ COMMITTED` behavior, the conditional update evaluates the stock condition against the current row state.

The inventory update, reservation creation, and order-state transition will occur within the same ACID transaction. A successful transaction must reserve the entire requested quantity and create exactly one authoritative reservation for the logical order. If any operation fails, the transaction will roll back completely.

The application will enforce uniqueness through stable order, attempt, and reservation identifiers. A duplicate message or retry must retrieve the existing allocation rather than decrementing inventory again. Since customers are allowed to purchase multiple phones, uniqueness constraints must not incorrectly limit them to one unit per order. Similarly, the system should not impose a unique `(sale_id, buyer_id)` order constraint unless the business explicitly limits customers to one order per sale.

The design uses an all-or-nothing reservation policy. If a customer requests five phones but only three remain, the database will not allocate any units to that request. This prevents unintended partial orders and keeps payment amounts consistent with the reserved quantity.

Although the conditional update uses a row lock internally, the lock is held only for a short database transaction. The goal is not to eliminate locking completely, but to avoid allowing all 100,000 checkout requests to compete directly for the same inventory row. The bounded worker pool introduced in Phase B controls this contention.

## C3. Redis-Based Provisional Reservation Control

Redis can be introduced as a fast provisional reservation layer to reduce unnecessary PostgreSQL operations. Before attempting an authoritative database reservation, a worker may acquire a temporary permit representing the requested quantity.

A Redis Lua script can check the provisional stock counter and decrement it atomically when enough permits remain.

```
-- KEYS[1] = provisional permit counter
-- KEYS[2] = unique token key
-- ARGV[1] = requested quantity
-- ARGV[2] = token TTL in milliseconds

local existing = redis.call("GET", KEYS[2])

if existing then
    return "EXISTS"
end

local stock = tonumber(redis.call("GET", KEYS[1]) or "0")
local quantity = tonumber(ARGV[1])

if not quantity or quantity < 1 or quantity % 1 ~= 0 then
    return "INVALID_QUANTITY"
end

if stock < quantity then
    return "INSUFFICIENT"
end

redis.call("DECRBY", KEYS[1], quantity)
redis.call("SET", KEYS[2], tostring(quantity), "PX", ARGV[2])

return "ACQUIRED"
```

The script executes atomically on the Redis primary responsible for the keys, preventing concurrent executions from interleaving their counter updates. In Redis Cluster, both keys must belong to the same hash slot.

However, Redis Lua atomicity does not guarantee durable inventory correctness across server crashes, replication delays, or failover. A provisional token may also expire while a worker is still processing, and token expiry does not automatically restore the counter. Therefore, the script is only a fast filtering mechanism and is not sufficient to prevent overselling independently.

After obtaining a Redis permit, the worker must still execute the PostgreSQL conditional reservation transaction. If the database rejects the allocation, the provisional permit must be released through an ownership-checked operation. Release logic must account for the original quantity and prevent duplicate releases from incorrectly increasing the counter.

Redis recovery will use reconciliation against PostgreSQL's authoritative inventory and reservation records. During Redis failover or counter rebuilding, provisional acquisition should be paused or fenced so that stale workers cannot interfere with the new permit state. PostgreSQL will continue to enforce the stock ceiling regardless of Redis inconsistencies.

Redis is optional for correctness. If performance testing demonstrates that a bounded worker pool and PostgreSQL conditional updates can process the necessary reservation workload efficiently, the provisional Redis layer may be removed to simplify the architecture.

## C4. Reservation Expiry and Inventory Release

Successful inventory reservations will have an authoritative expiration timestamp stored in PostgreSQL. A provisional five-minute reservation period may be used initially, but the actual duration must be determined using payment completion times, customer experience requirements, and supported payment methods.

When a reservation expires without a valid completed payment, an expiry worker will execute a database transaction that verifies the reservation state, marks it as expired, updates the associated order, and returns the reserved quantity to available inventory.

The inventory release operation will use the quantity stored in the reservation:

```
UPDATE sale_inventory
SET reserved = reserved - $2,
    available = available + $2
WHERE sale_id = $1
  AND reserved >= $2;
```

This update must occur only after the corresponding reservation has successfully transitioned from an active state to an expired or cancelled state within the same transaction. Duplicate expiry processing must not release the same quantity twice.

One important failure scenario occurs when payment succeeds at approximately the same time as reservation expiry. If the expiry worker releases the stock while a delayed payment callback confirms the old order, the same inventory could be allocated to another customer.

To prevent this, payment confirmation and reservation expiry must use mutually exclusive, database-controlled state transitions. Both operations will inspect and lock the same authoritative reservation record, ensuring that only one transition succeeds.

If a reservation has already expired, a late payment success must not automatically reactivate it or confirm the order. The system must instead verify the payment and initiate an appropriate void, refund, or reconciliation process. Payment methods that can settle long after the reservation deadline require a different reservation policy.

## C5. Order, Payment, and Fulfillment Consistency

The system will maintain separate state transitions for checkout attempts, inventory reservations, payments, orders, and fulfillment. This separation prevents an accepted checkout request from being incorrectly treated as a successful purchase.

An accepted checkout attempt initially enters the queued state. When selected for processing, it moves to reserving. A successful PostgreSQL allocation creates an active reservation and moves the order to reserved. Payment initiation then moves the order into a payment-pending state.

The payment provider will be called using a stable idempotency identifier so that retries do not unintentionally create additional charges. If the provider request times out, the payment orchestrator will retrieve the authoritative payment status rather than immediately submitting another independent charge.

Once payment success has been verified, a short PostgreSQL transaction will mark the reservation as consumed, transfer the reserved quantity to sold inventory, confirm the order, record the payment result, and create an outbox event for fulfillment.

The inventory update is:

```
UPDATE sale_inventory
SET reserved = reserved - $2,
    sold = sold + $2
WHERE sale_id = $1
  AND reserved >= $2;
```

The transaction must verify that the reservation was active and that its transition to consumed succeeded exactly once. A duplicate payment webhook must return the existing confirmation result without moving inventory again.

Fulfillment will begin only after the confirmed-order event is durably published. The fulfillment service will use its own idempotency controls to prevent duplicate shipments if the event is delivered more than once.

## C6. Locking Strategies and Database Performance

Several concurrency-control techniques can be used in the reservation workflow. Pessimistic locking explicitly locks records before modifying them, while optimistic locking relies on version checks and retries. Atomic conditional updates combine the inventory condition and mutation in one database statement.

| Strategy                   | Advantages                                      | Limitations                                | Recommended use                       |
| -------------------------- | ----------------------------------------------- | ------------------------------------------ | ------------------------------------- |
| Pessimistic locking        | Predictable state transitions                   | Lock contention and waiting                | Reservation and payment state changes |
| Optimistic locking         | Efficient under low contention                  | High retry rates under heavy contention    | Individual order records              |
| Atomic conditional updates | Enforces stock availability during modification | Serializes conflicting inventory updates   | Shared inventory counter              |
| Redis Lua                  | Fast atomic provisional checks                  | Does not guarantee cross-system durability | Optional permit filtering             |

For the shared inventory record, atomic conditional updates with bounded worker concurrency are preferred. Unrestricted optimistic retries could generate a retry storm, where thousands of transactions repeatedly fail version checks and increase database load.

Database efficiency will also depend on narrow queries, appropriate indexes, and connection pooling. Unique indexes will protect logical attempt identifiers, reservation-to-order relationships, and provider payment identifiers. A partial index on active reservation expiry timestamps will support efficient expiry processing.

The checkout transaction will avoid unnecessary joins, repeated queries for related records, and external network calls. Product pricing and sale configuration should be validated and captured before allocation, rather than repeatedly loading unrelated catalog information during the critical transaction.

PgBouncer or an equivalent connection-pooling mechanism may be used to control the total number of PostgreSQL connections. API and worker connection limits must be configured together so that horizontal scaling does not overwhelm the database.

The principal trade-off of this design is reduced parallelism at the authoritative inventory row. This is acceptable because strict consistency is more important than maximizing concurrent inventory writes for a sale containing only 500 units.

# Phase D: Capacity Planning and Resilience

## D1. Throughput Estimation and Capacity Planning

The flash sale involves two distinct workloads: accepting a large number of checkout requests and allocating a limited number of smartphones. These workloads have different performance requirements and must be scaled independently.

The admission layer may receive more than 100,000 requests within ten seconds, while the inventory layer can initially allocate no more than 500 units. Additional reservation transactions may occur when customers cancel, fail payment, or allow their reservations to expire.

The average arrival rate for 100,000 requests over ten seconds is 10,000 requests per second. However, the infrastructure must be designed for the expected peak rate, which could be substantially higher if most users click the checkout button during the first second.

A simplified API capacity model is:

\\[ N\_{\text{replicas}} \geq \frac{\lambda\_{\text{peak}}}{R\_{\text{replica}}}\times H \\]

Where:

- \\(\lambda\_{\text{peak}}\\) is the expected peak incoming request rate.
- \\(R\_{\text{replica}}\\) is the measured sustainable request rate per healthy API replica.
- \\(H\\) is the capacity headroom multiplier.

For example, if load testing demonstrates that one API replica can sustain 2,000 durable admission requests per second and the expected peak is 20,000 requests per second, at least ten replicas would be needed before additional headroom. These figures are illustrative and cannot be used as final deployment settings without benchmarking.

Capacity planning will cover the complete request-processing pipeline rather than focusing only on API server throughput.

| Component                   | Capacity requirement                               | Validation method                      |
| --------------------------- | -------------------------------------------------- | -------------------------------------- |
| CDN and WAF                 | Handle burst traffic and concurrent connections    | Edge load testing                      |
| Load balancer               | Distribute peak requests without excessive latency | Connection storm testing               |
| Admission API               | Sustain the target durable admission rate          | API and database load testing          |
| PostgreSQL admission ledger | Commit accepted attempts reliably                  | Transaction throughput benchmark       |
| Kafka                       | Store and deliver the expected event backlog       | Broker throughput and recovery testing |
| Reservation workers         | Process eligible attempts at a controlled rate     | Worker concurrency testing             |
| PostgreSQL inventory        | Perform atomic quantity-based reservations         | Transaction and lock-wait testing      |
| Status service              | Handle large-scale client polling                  | Read throughput testing                |
| Payment provider            | Support the expected payment workload              | Provider limit and integration testing |

The admission ledger must be able to durably commit requests at the required rate. If it cannot, requests will need to be throttled or rejected before acceptance. The system must never return a successful admission response for a request that has not crossed the defined durability boundary.

## D2. Queue Management and Backpressure

Kafka will absorb the difference between the rate at which checkout requests are accepted and the rate at which backend workers process them. This allows the system to handle a short burst without immediately overwhelming PostgreSQL or the payment provider.

Queue growth can be approximated by:

\\[ B(t)=B(0)+\int_0^t \left(\lambda_a(u)-\mu_w(u)\right)\\,du \\]

Here, \\(B(t)\\) represents the backlog at time \\(t\\), \\(\lambda_a\\) represents the accepted request arrival rate, and \\(\mu_w\\) represents the worker processing rate.

If accepted requests arrive faster than workers can process them, the backlog increases. Kafka must therefore have sufficient storage, retention, and recovery capacity for the expected workload. The maximum acceptable queue waiting time must also be defined as a business requirement.

The system will use bounded worker concurrency and a controlled reservation candidate window rather than sending every queued request to the inventory database. Once inventory is fully reserved, remaining customers may continue waiting according to the sale policy because some active reservations could expire or fail payment.

When inventory becomes available again, the scheduler can select the next eligible checkout attempt. The ordering policy must be stored durably so that retries, worker restarts, and Kafka partitioning do not unintentionally change customer priority.

Backpressure will be applied at multiple levels. Workers will pause message consumption when their local capacity is exhausted, while the admission API will reject or defer new requests when the durable admission system approaches its safe limits. This prevents overload from propagating through the entire architecture.

## D3. Autoscaling and Infrastructure Pre-Warming

Since the flash sale begins at a known time, infrastructure will be scaled and prepared before the event rather than relying entirely on reactive autoscaling.

API replicas, worker services, Kafka brokers, Redis connections, PostgreSQL connection pools, and monitoring infrastructure will be pre-warmed. CDN assets will be cached in advance where practical, and readiness checks will confirm that services can reach their required dependencies.

Reactive autoscaling remains useful for sustained traffic and unexpected variations, but it cannot be relied upon to handle the initial ten-second burst. Starting new containers, loading application images, establishing database connections, and passing readiness checks may take longer than the burst itself.

Autoscaling decisions will consider request latency, admission throughput, Kafka lag, worker processing time, database saturation, and connection-pool utilization. Increasing API replicas will not resolve a saturated admission database, and increasing reservation workers will not improve throughput if all workers are waiting for the same PostgreSQL inventory row.

Services will also support graceful shutdown. API instances will stop accepting new requests before termination, while workers will finish or safely abandon in-flight tasks so that Kafka can redeliver uncommitted messages.

## D4. Fault Tolerance and Recovery

The system will be designed to recover from common infrastructure and application failures without losing durably accepted checkout attempts or allocating inventory more than once.

If an API instance crashes before the admission transaction commits, the request will not be considered accepted. The client may safely retry using its existing idempotency key. If the API crashes after committing the transaction but before sending the response, the accepted attempt will remain in PostgreSQL and can be retrieved using the same key.

If Kafka becomes unavailable, the transactional outbox will retain unpublished events until the broker recovers. Previously accepted requests will remain durable, while new admission may be restricted if the backlog exceeds configured limits.

Worker crashes will be handled through transaction rollback, Kafka redelivery, and idempotent processing. If a worker fails before committing an inventory reservation, the database will not retain a partial allocation. If it fails after committing but before acknowledging Kafka, the redelivered event will retrieve the existing reservation instead of allocating inventory again.

A Redis outage may interrupt provisional permit acquisition, but it must not compromise inventory correctness. The system can pause allocation and reconcile Redis against PostgreSQL before resuming. Redis failures must never allow the reservation workers to bypass the authoritative inventory condition.

PostgreSQL failures require a carefully configured durability and failover strategy. Replication, durable write acknowledgments, backup procedures, and recovery testing must match the infrastructure failures the business expects to tolerate. Backups alone are insufficient to guarantee that every acknowledged transaction survives a failover.

Payment failures will be handled through provider idempotency, status verification, and reconciliation. A delayed or duplicated payment callback must not confirm an expired reservation or create another inventory allocation. When payment succeeds after a reservation has been released, the system must initiate the appropriate refund or void process rather than overselling.

The reliability objective is that every durably accepted checkout attempt remains recoverable and eventually reaches a recorded outcome within the configured infrastructure failure model and retention period. This is a more precise guarantee than claiming that no data can ever be lost under any possible disaster.

## D5. Performance Testing and Validation

The proposed architecture must be validated using production-like load tests before deployment. The main test scenario will generate more than 100,000 checkout requests within ten seconds using an open-loop load generator. This approach maintains the intended arrival rate even when the system slows down, making it more representative of an actual flash-sale burst.

Testing will measure admission throughput, p50, p95, and p99 response latency, rejection rates, database commit latency, Kafka publishing delay, worker processing rates, queue waiting time, and order completion time. Checkout admission latency will be measured separately from inventory reservation and payment completion because these operations have different performance objectives.

Correctness tests will submit far more requests than the available inventory and verify that the total quantity reserved and sold never exceeds 500. Tests must include different order quantities, simultaneous requests for the remaining stock, duplicate idempotency keys, retries after timeouts, and overlapping payment and expiry operations.

Failure-injection tests will simulate API crashes, Kafka outages, worker restarts, Redis failover, PostgreSQL transaction failures, delayed payment callbacks, and duplicate webhook deliveries. The system must demonstrate that these failures do not cause duplicate authoritative allocations and that accepted attempts can be recovered.

Frontend performance will be validated under the defined mobile device, network, and cache conditions. Status polling will be tested separately at the expected number of active clients, since polling traffic may exceed the initial admission rate.

## D6. Monitoring and Operational Controls

Production monitoring will provide visibility into the complete checkout pipeline. Dashboards will track incoming request rates, admission commit latency, accepted and rejected attempts, Kafka and outbox backlog, worker processing time, database lock waits, connection-pool utilization, reservation expiry rates, payment reconciliation status, and status projection delays.

Inventory correctness will be monitored continuously using the conservation equation:

\\[ total\\\_stock-(available+reserved+sold)=0 \\]

Any nonzero result will be treated as a critical integrity incident. Additional reconciliation checks will compare active reservation records with the reserved quantity and confirmed orders with the sold quantity. These checks are necessary because a balanced inventory counter alone cannot detect every incorrect order transition.

Operational controls will include the ability to pause checkout admission, stop new inventory reservations, adjust worker concurrency, suppress retries, and enter a reconciliation-only recovery mode. These controls will allow operators to respond to unexpected failures without deleting or silently discarding previously accepted attempts.

The architecture deliberately prioritizes inventory correctness and recoverability over unrestricted parallel processing. Its principal costs are the additional admission ledger, messaging infrastructure, asynchronous workflow coordination, and operational monitoring. These components provide greater control during traffic bursts, but their necessity and capacity must be confirmed through testing before production deployment.

These four phases establish the proposed technical architecture: Phase A handles client responsiveness and safe request submission, Phase B manages durable admission and asynchronous backend processing, Phase C enforces inventory and payment consistency, and Phase D addresses scalability, recovery, and operational validation.

The subsequent End-to-End Request Lifecycle, Failure Analysis, and Requirement Validation sections can reference these phases without repeating their implementation details.

## C5. Order, Payment, and Fulfillment Consistency

The system will maintain separate state transitions for checkout attempts, inventory reservations, payments, orders, and fulfillment. This separation prevents an accepted checkout request from being incorrectly treated as a successful purchase.

An accepted checkout attempt initially enters the queued state. When selected for processing, it moves to reserving. A successful PostgreSQL allocation creates an active reservation and moves the order to reserved. Payment initiation then moves the order into a payment-pending state.

The payment provider will be called using a stable idempotency identifier so that retries do not unintentionally create additional charges. If the provider request times out, the payment orchestrator will retrieve the authoritative payment status rather than immediately submitting another independent charge.

Once payment success has been verified, a short PostgreSQL transaction will mark the reservation as consumed, transfer the reserved quantity to sold inventory, confirm the order, record the payment result, and create an outbox event for fulfillment.

The inventory update is:

```
UPDATE sale_inventory
SET reserved = reserved - $2,
    sold = sold + $2
WHERE sale_id = $1
  AND reserved >= $2;

```

The transaction must verify that the reservation was active and that its transition to consumed succeeded exactly once. A duplicate payment webhook must return the existing confirmation result without moving inventory again.

Fulfillment will begin only after the confirmed-order event is durably published. The fulfillment service will use its own idempotency controls to prevent duplicate shipments if the event is delivered more than once.

## C6. Locking Strategies and Database Performance

Several concurrency-control techniques can be used in the reservation workflow. Pessimistic locking explicitly locks records before modifying them, while optimistic locking relies on version checks and retries. Atomic conditional updates combine the inventory condition and mutation in one database statement.

| Strategy | Advantages | Limitations | Recommended use |
| --- | --- | --- | --- |
| Pessimistic locking | Predictable state transitions | Lock contention and waiting | Reservation and payment state changes |
| Optimistic locking | Efficient under low contention | High retry rates under heavy contention | Individual order records |
| Atomic conditional updates | Enforces stock availability during modification | Serializes conflicting inventory updates | Shared inventory counter |
| Redis Lua | Fast atomic provisional checks | Does not guarantee cross-system durability | Optional permit filtering |

For the shared inventory record, atomic conditional updates with bounded worker concurrency are preferred. Unrestricted optimistic retries could generate a retry storm, where thousands of transactions repeatedly fail version checks and increase database load.

Database efficiency will also depend on narrow queries, appropriate indexes, and connection pooling. Unique indexes will protect logical attempt identifiers, reservation-to-order relationships, and provider payment identifiers. A partial index on active reservation expiry timestamps will support efficient expiry processing.

The checkout transaction will avoid unnecessary joins, repeated queries for related records, and external network calls. Product pricing and sale configuration should be validated and captured before allocation, rather than repeatedly loading unrelated catalog information during the critical transaction.

PgBouncer or an equivalent connection-pooling mechanism may be used to control the total number of PostgreSQL connections. API and worker connection limits must be configured together so that horizontal scaling does not overwhelm the database.

The principal trade-off of this design is reduced parallelism at the authoritative inventory row. This is acceptable because strict consistency is more important than maximizing concurrent inventory writes for a sale containing only 500 units.

# Phase D: Capacity Planning and Resilience

## D1. Throughput Estimation and Capacity Planning

The flash sale involves two distinct workloads: accepting a large number of checkout requests and allocating a limited number of smartphones. These workloads have different performance requirements and must be scaled independently.

The admission layer may receive more than 100,000 requests within ten seconds, while the inventory layer can initially allocate no more than 500 units. Additional reservation transactions may occur when customers cancel, fail payment, or allow their reservations to expire.

The average arrival rate for 100,000 requests over ten seconds is 10,000 requests per second. However, the infrastructure must be designed for the expected peak rate, which could be substantially higher if most users click the checkout button during the first second.

A simplified API capacity model is:

$$N_{\text{replicas}} \geq \frac{\lambda_{\text{peak}}}{R_{\text{replica}}}\times H$$

Where:

* $\lambda_{\text{peak}}$ is the expected peak incoming request rate.
* $R_{\text{replica}}$ is the measured sustainable request rate per healthy API replica.
* $H$ is the capacity headroom multiplier.

For example, if load testing demonstrates that one API replica can sustain 2,000 durable admission requests per second and the expected peak is 20,000 requests per second, at least ten replicas would be needed before additional headroom. These figures are illustrative and cannot be used as final deployment settings without benchmarking.

Capacity planning will cover the complete request-processing pipeline rather than focusing only on API server throughput.

| Component | Capacity requirement | Validation method |
| --- | --- | --- |
| CDN and WAF | Handle burst traffic and concurrent connections | Edge load testing |
| Load balancer | Distribute peak requests without excessive latency | Connection storm testing |
| Admission API | Sustain the target durable admission rate | API and database load testing |
| PostgreSQL admission ledger | Commit accepted attempts reliably | Transaction throughput benchmark |
| Kafka | Store and deliver the expected event backlog | Broker throughput and recovery testing |
| Reservation workers | Process eligible attempts at a controlled rate | Worker concurrency testing |
| PostgreSQL inventory | Perform atomic quantity-based reservations | Transaction and lock-wait testing |
| Status service | Handle large-scale client polling | Read throughput testing |
| Payment provider | Support the expected payment workload | Provider limit and integration testing |

The admission ledger must be able to durably commit requests at the required rate. If it cannot, requests will need to be throttled or rejected before acceptance. The system must never return a successful admission response for a request that has not crossed the defined durability boundary.

## D2. Queue Management and Backpressure

Kafka will absorb the difference between the rate at which checkout requests are accepted and the rate at which backend workers process them. This allows the system to handle a short burst without immediately overwhelming PostgreSQL or the payment provider.

Queue growth can be approximated by:

$$B(t)=B(0)+\int_0^t \left(\lambda_a(u)-\mu_w(u)\right)\,du$$

Here, $B(t)$ represents the backlog at time $t$, $\lambda_a$ represents the accepted request arrival rate, and $\mu_w$ represents the worker processing rate.

If accepted requests arrive faster than workers can process them, the backlog increases. Kafka must therefore have sufficient storage, retention, and recovery capacity for the expected workload. The maximum acceptable queue waiting time must also be defined as a business requirement.

The system will use bounded worker concurrency and a controlled reservation candidate window rather than sending every queued request to the inventory database. Once inventory is fully reserved, remaining customers may continue waiting according to the sale policy because some active reservations could expire or fail payment.

When inventory becomes available again, the scheduler can select the next eligible checkout attempt. The ordering policy must be stored durably so that retries, worker restarts, and Kafka partitioning do not unintentionally change customer priority.

Backpressure will be applied at multiple levels. Workers will pause message consumption when their local capacity is exhausted, while the admission API will reject or defer new requests when the durable admission system approaches its safe limits. This prevents overload from propagating through the entire architecture.

## D3. Autoscaling and Infrastructure Pre-Warming

Since the flash sale begins at a known time, infrastructure will be scaled and prepared before the event rather than relying entirely on reactive autoscaling.

API replicas, worker services, Kafka brokers, Redis connections, PostgreSQL connection pools, and monitoring infrastructure will be pre-warmed. CDN assets will be cached in advance where practical, and readiness checks will confirm that services can reach their required dependencies.

Reactive autoscaling remains useful for sustained traffic and unexpected variations, but it cannot be relied upon to handle the initial ten-second burst. Starting new containers, loading application images, establishing database connections, and passing readiness checks may take longer than the burst itself.

Autoscaling decisions will consider request latency, admission throughput, Kafka lag, worker processing time, database saturation, and connection-pool utilization. Increasing API replicas will not resolve a saturated admission database, and increasing reservation workers will not improve throughput if all workers are waiting for the same PostgreSQL inventory row.

Services will also support graceful shutdown. API instances will stop accepting new requests before termination, while workers will finish or safely abandon in-flight tasks so that Kafka can redeliver uncommitted messages.

## D4. Fault Tolerance and Recovery

The system will be designed to recover from common infrastructure and application failures without losing durably accepted checkout attempts or allocating inventory more than once.

If an API instance crashes before the admission transaction commits, the request will not be considered accepted. The client may safely retry using its existing idempotency key. If the API crashes after committing the transaction but before sending the response, the accepted attempt will remain in PostgreSQL and can be retrieved using the same key.

If Kafka becomes unavailable, the transactional outbox will retain unpublished events until the broker recovers. Previously accepted requests will remain durable, while new admission may be restricted if the backlog exceeds configured limits.

Worker crashes will be handled through transaction rollback, Kafka redelivery, and idempotent processing. If a worker fails before committing an inventory reservation, the database will not retain a partial allocation. If it fails after committing but before acknowledging Kafka, the redelivered event will retrieve the existing reservation instead of allocating inventory again.

A Redis outage may interrupt provisional permit acquisition, but it must not compromise inventory correctness. The system can pause allocation and reconcile Redis against PostgreSQL before resuming. Redis failures must never allow the reservation workers to bypass the authoritative inventory condition.

PostgreSQL failures require a carefully configured durability and failover strategy. Replication, durable write acknowledgments, backup procedures, and recovery testing must match the infrastructure failures the business expects to tolerate. Backups alone are insufficient to guarantee that every acknowledged transaction survives a failover.

Payment failures will be handled through provider idempotency, status verification, and reconciliation. A delayed or duplicated payment callback must not confirm an expired reservation or create another inventory allocation. When payment succeeds after a reservation has been released, the system must initiate the appropriate refund or void process rather than overselling.

The reliability objective is that every durably accepted checkout attempt remains recoverable and eventually reaches a recorded outcome within the configured infrastructure failure model and retention period. This is a more precise guarantee than claiming that no data can ever be lost under any possible disaster.

## D5. Performance Testing and Validation

The proposed architecture must be validated using production-like load tests before deployment. The main test scenario will generate more than 100,000 checkout requests within ten seconds using an open-loop load generator. This approach maintains the intended arrival rate even when the system slows down, making it more representative of an actual flash-sale burst.

Testing will measure admission throughput, p50, p95, and p99 response latency, rejection rates, database commit latency, Kafka publishing delay, worker processing rates, queue waiting time, and order completion time. Checkout admission latency will be measured separately from inventory reservation and payment completion because these operations have different performance objectives.

Correctness tests will submit far more requests than the available inventory and verify that the total quantity reserved and sold never exceeds 500. Tests must include different order quantities, simultaneous requests for the remaining stock, duplicate idempotency keys, retries after timeouts, and overlapping payment and expiry operations.

Failure-injection tests will simulate API crashes, Kafka outages, worker restarts, Redis failover, PostgreSQL transaction failures, delayed payment callbacks, and duplicate webhook deliveries. The system must demonstrate that these failures do not cause duplicate authoritative allocations and that accepted attempts can be recovered.

Frontend performance will be validated under the defined mobile device, network, and cache conditions. Status polling will be tested separately at the expected number of active clients, since polling traffic may exceed the initial admission rate.

## D6. Monitoring and Operational Controls

Production monitoring will provide visibility into the complete checkout pipeline. Dashboards will track incoming request rates, admission commit latency, accepted and rejected attempts, Kafka and outbox backlog, worker processing time, database lock waits, connection-pool utilization, reservation expiry rates, payment reconciliation status, and status projection delays.

Inventory correctness will be monitored continuously using the conservation equation:

$$\text{total\_stock} - (\text{available} + \text{reserved} + \text{sold}) = 0$$

Any nonzero result will be treated as a critical integrity incident. Additional reconciliation checks will compare active reservation records with the reserved quantity and confirmed orders with the sold quantity. These checks are necessary because a balanced inventory counter alone cannot detect every incorrect order transition.

Operational controls will include the ability to pause checkout admission, stop new inventory reservations, adjust worker concurrency, suppress retries, and enter a reconciliation-only recovery mode. These controls will allow operators to respond to unexpected failures without deleting or silently discarding previously accepted attempts.

The architecture deliberately prioritizes inventory correctness and recoverability over unrestricted parallel processing. Its principal costs are the additional admission ledger, messaging infrastructure, asynchronous workflow coordination, and operational monitoring. These components provide greater control during traffic bursts, but their necessity and capacity must be confirmed through testing before production deployment.

These four phases establish the proposed technical architecture: Phase A handles client responsiveness and safe request submission, Phase B manages durable admission and asynchronous backend processing, Phase C enforces inventory and payment consistency, and Phase D addresses scalability, recovery, and operational validation.

The subsequent End-to-End Request Lifecycle, Failure Analysis, and Requirement Validation sections can reference these phases without repeating their implementation details.
