CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY);
CREATE TABLE sale_inventory (
 sale_id text PRIMARY KEY CHECK (sale_id = 'festival-sale'),
 sku text NOT NULL CHECK (sku = 'phone-001'),
 total_stock integer NOT NULL CHECK (total_stock = 500),
 available integer NOT NULL CHECK (available >= 0),
 reserved integer NOT NULL CHECK (reserved >= 0),
 sold integer NOT NULL CHECK (sold >= 0),
 CHECK (available + reserved + sold = total_stock)
);
INSERT INTO sale_inventory VALUES ('festival-sale','phone-001',500,500,0,0);
CREATE TABLE attempts (
 id uuid PRIMARY KEY,
 sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
 buyer text NOT NULL,
 idempotency_key text NOT NULL,
 sale_id text NOT NULL REFERENCES sale_inventory,
 sku text NOT NULL CHECK (sku = 'phone-001'),
 quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 500),
 unit_price integer NOT NULL DEFAULT 39900 CHECK (unit_price = 39900),
 status text NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED','RESERVED','PAYMENT_PENDING','CONFIRMED','SOLD_OUT','FAILED')),
 reason text,
 version integer NOT NULL DEFAULT 1,
 dispatched boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE (buyer,idempotency_key)
);
CREATE INDEX attempts_queue ON attempts(sequence) WHERE status='QUEUED';
CREATE INDEX attempts_pending ON attempts(status) WHERE status IN ('QUEUED','RESERVED','PAYMENT_PENDING');
CREATE TABLE reservations (
 attempt_id uuid PRIMARY KEY REFERENCES attempts,
 quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 500),
 state text NOT NULL CHECK (state IN ('ACTIVE','CONSUMED','EXPIRED','RELEASED')),
 expires_at timestamptz NOT NULL
);
CREATE INDEX reservation_expiry ON reservations(expires_at) WHERE state='ACTIVE';
CREATE TABLE payments (
 attempt_id uuid PRIMARY KEY REFERENCES attempts,
 provider_id uuid NOT NULL UNIQUE,
 outcome text NOT NULL CHECK (outcome IN ('success','failure')),
 state text NOT NULL CHECK (state IN ('PENDING','SUCCEEDED','FAILED','REFUND_PENDING','REFUNDED')),
 due_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX payment_due ON payments(due_at) WHERE state='PENDING';
CREATE TABLE outbox (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 kind text NOT NULL CHECK (kind IN ('admitted','confirmed')),
 attempt_id uuid NOT NULL REFERENCES attempts,
 published_at timestamptz,
 failures integer NOT NULL DEFAULT 0,
 next_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_error text,
 UNIQUE (kind,attempt_id)
);
CREATE INDEX outbox_pending ON outbox(next_at,id) WHERE published_at IS NULL;
CREATE TABLE inbox (event_id bigint PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE fulfillments (attempt_id uuid PRIMARY KEY REFERENCES attempts, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE dead_letters (topic text NOT NULL, partition_id integer NOT NULL, offset_id bigint NOT NULL, payload text NOT NULL, reason text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(topic,partition_id,offset_id));
