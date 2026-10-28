import uuid

from app import config
from app.db import transaction

TERMINAL = {'CONFIRMED', 'SOLD_OUT', 'FAILED'}


class Conflict(Exception):
    pass


class Overloaded(Exception):
    pass


def transition(conn, attempt_id, status, reason=None):
    conn.execute('''UPDATE attempts SET status=%s, reason=%s, version=version+1,
                    updated_at=clock_timestamp() WHERE id=%s''', (status, reason, attempt_id))


def admit(buyer, key, sale_id, sku, quantity):
    with transaction() as conn:
        # Serializes capacity checks and duplicate admission, independently of inventory.
        conn.execute('SELECT pg_advisory_xact_lock(101)')
        existing = conn.execute('SELECT * FROM attempts WHERE buyer=%s AND idempotency_key=%s', (buyer, key)).fetchone()
        if existing:
            if (existing['sale_id'], existing['sku'], existing['quantity']) != (sale_id, sku, quantity):
                raise Conflict('Idempotency key belongs to different checkout details')
            return str(existing['id'])
        if config.ADMISSION_PAUSED or config.RECONCILIATION_ONLY:
            raise Overloaded('Admission is paused')
        count = conn.execute("SELECT count(*) AS n FROM attempts WHERE status IN ('QUEUED','RESERVED','PAYMENT_PENDING')").fetchone()['n']
        unpublished = conn.execute('SELECT count(*) AS n FROM outbox WHERE published_at IS NULL').fetchone()['n']
        if count >= config.MAX_PENDING or unpublished >= config.MAX_PENDING:
            raise Overloaded('Durable queue is at capacity')
        attempt_id = uuid.uuid4()
        conn.execute('''INSERT INTO attempts(id,buyer,idempotency_key,sale_id,sku,quantity)
                        VALUES (%s,%s,%s,%s,%s,%s)''', (attempt_id, buyer, key, sale_id, sku, quantity))
        conn.execute("INSERT INTO outbox(kind,attempt_id) VALUES ('admitted',%s)", (attempt_id,))
    return str(attempt_id)


def status(attempt_id, buyer):
    with transaction() as conn:
        row = conn.execute('''SELECT a.*,r.state AS reservation_state,r.expires_at,
                              p.state AS payment_state,p.provider_id,
                              clock_timestamp() AS server_time
                              FROM attempts a LEFT JOIN reservations r ON r.attempt_id=a.id
                              LEFT JOIN payments p ON p.attempt_id=a.id
                              WHERE a.id=%s AND a.buyer=%s''', (attempt_id, buyer)).fetchone()
    if not row:
        return None
    return {key: str(value) if isinstance(value, uuid.UUID) else value for key, value in row.items()
            if key not in ('idempotency_key', 'dispatched', 'buyer', 'sequence')}


def receive(event):
    if not isinstance(event, dict) or not isinstance(event.get('eventId'), int) or not isinstance(event.get('attemptId'), str):
        raise ValueError('Invalid event envelope')
    event_id = int(event['eventId'])
    attempt_id = uuid.UUID(event['attemptId'])
    kind = event['kind']
    if kind not in ('admitted', 'confirmed'):
        raise ValueError('Unsupported event kind')
    with transaction() as conn:
        # Validate against the durable source; Kafka payloads cannot invent orders.
        source = conn.execute('SELECT * FROM outbox WHERE id=%s', (event_id,)).fetchone()
        if not source or source['kind'] != kind or source['attempt_id'] != attempt_id:
            raise ValueError('Event does not match outbox')
        inserted = conn.execute('INSERT INTO inbox(event_id) VALUES (%s) ON CONFLICT DO NOTHING RETURNING event_id', (event_id,)).fetchone()
        if not inserted:
            return
        if kind == 'admitted':
            conn.execute('UPDATE attempts SET dispatched=true WHERE id=%s', (attempt_id,))
        else:
            conn.execute("INSERT INTO fulfillments(attempt_id) SELECT id FROM attempts WHERE id=%s AND status='CONFIRMED' ON CONFLICT DO NOTHING", (attempt_id,))


def allocate_next():
    if config.RESERVATIONS_PAUSED or config.RECONCILIATION_ONLY:
        return False
    with transaction() as conn:
        # A sale scheduler owns priority; Kafka partitions never determine fairness.
        if not conn.execute('SELECT pg_try_advisory_xact_lock(102) AS locked').fetchone()['locked']:
            return False
        attempt = conn.execute("SELECT * FROM attempts WHERE status='QUEUED' ORDER BY sequence LIMIT 1 FOR UPDATE").fetchone()
        if not attempt or not attempt['dispatched']:
            return False
        attempt_id, quantity = attempt['id'], attempt['quantity']
        allocated = conn.execute('''UPDATE sale_inventory SET available=available-%s,reserved=reserved+%s
                                    WHERE sale_id=%s AND available >= %s RETURNING *''',
                                 (quantity, quantity, attempt['sale_id'], quantity)).fetchone()
        if not allocated:
            inventory = conn.execute('SELECT * FROM sale_inventory WHERE sale_id=%s', (attempt['sale_id'],)).fetchone()
            if inventory['reserved'] > 0:
                return False
            transition(conn, attempt_id, 'SOLD_OUT', 'Full requested quantity is unavailable')
            return True
        conn.execute("INSERT INTO reservations VALUES (%s,%s,'ACTIVE',clock_timestamp() + %s * interval '1 second')",
                     (attempt_id, quantity, config.RESERVATION_SECONDS))
        transition(conn, attempt_id, 'RESERVED')
    return True


def start_payment(attempt_id, buyer, outcome, delay):
    with transaction() as conn:
        attempt = conn.execute('SELECT * FROM attempts WHERE id=%s AND buyer=%s FOR UPDATE', (attempt_id, buyer)).fetchone()
        if not attempt:
            return None
        existing = conn.execute('SELECT * FROM payments WHERE attempt_id=%s', (attempt_id,)).fetchone()
        if existing:
            if existing['outcome'] != outcome:
                raise Conflict('Payment already started with a different outcome')
            return str(existing['provider_id'])
        reservation = conn.execute('SELECT *,expires_at>clock_timestamp() AS valid FROM reservations WHERE attempt_id=%s FOR UPDATE', (attempt_id,)).fetchone()
        if not reservation or reservation['state'] != 'ACTIVE' or not reservation['valid']:
            raise Conflict('Reservation is unavailable or has expired')
        provider_id = uuid.uuid4()
        # This durable table IS the simulated provider, including its stable identity.
        conn.execute("INSERT INTO payments VALUES (%s,%s,%s,'PENDING',clock_timestamp()+%s * interval '1 second',clock_timestamp())",
                     (attempt_id, provider_id, outcome, delay))
        transition(conn, attempt_id, 'PAYMENT_PENDING')
    return str(provider_id)


def release(conn, attempt, reservation, expired):
    state = 'EXPIRED' if expired else 'RELEASED'
    conn.execute('UPDATE reservations SET state=%s WHERE attempt_id=%s', (state, attempt['id']))
    changed = conn.execute('''UPDATE sale_inventory SET reserved=reserved-%s,available=available+%s
                              WHERE sale_id=%s AND reserved >= %s RETURNING sale_id''',
                           (reservation['quantity'], reservation['quantity'], attempt['sale_id'], reservation['quantity'])).fetchone()
    if not changed:
        raise RuntimeError('Inventory release invariant violated')
    transition(conn, attempt['id'], 'FAILED', 'Reservation expired' if expired else 'Payment declined')


def complete_payment(attempt_id):
    with transaction() as conn:
        attempt = conn.execute('SELECT * FROM attempts WHERE id=%s FOR UPDATE', (attempt_id,)).fetchone()
        if not attempt:
            return False
        reservation = conn.execute('SELECT *,expires_at<=clock_timestamp() AS expired FROM reservations WHERE attempt_id=%s FOR UPDATE', (attempt_id,)).fetchone()
        payment = conn.execute("SELECT *,due_at<=clock_timestamp() AS due FROM payments WHERE attempt_id=%s FOR UPDATE", (attempt_id,)).fetchone()
        if not payment or payment['state'] != 'PENDING' or not payment['due']:
            return False
        if reservation['state'] == 'ACTIVE' and reservation['expired']:
            release(conn, attempt, reservation, True)
            reservation['state'] = 'EXPIRED'
        if payment['outcome'] == 'success' and reservation['state'] == 'ACTIVE':
            changed = conn.execute('''UPDATE sale_inventory SET reserved=reserved-%s,sold=sold+%s
                                      WHERE sale_id=%s AND reserved >= %s RETURNING sale_id''',
                                   (reservation['quantity'], reservation['quantity'], attempt['sale_id'], reservation['quantity'])).fetchone()
            if not changed:
                raise RuntimeError('Inventory consumption invariant violated')
            conn.execute("UPDATE reservations SET state='CONSUMED' WHERE attempt_id=%s", (attempt_id,))
            transition(conn, attempt_id, 'CONFIRMED')
            conn.execute("INSERT INTO outbox(kind,attempt_id) VALUES ('confirmed',%s) ON CONFLICT DO NOTHING", (attempt_id,))
            payment_state = 'SUCCEEDED'
        elif payment['outcome'] == 'success':
            payment_state = 'REFUND_PENDING'
        else:
            if reservation['state'] == 'ACTIVE':
                release(conn, attempt, reservation, False)
            payment_state = 'FAILED'
        conn.execute('UPDATE payments SET state=%s,updated_at=clock_timestamp() WHERE attempt_id=%s', (payment_state, attempt_id))
        conn.execute('UPDATE attempts SET version=version+1,updated_at=clock_timestamp() WHERE id=%s', (attempt_id,))
    return True


def expire_one(attempt_id):
    with transaction() as conn:
        attempt = conn.execute('SELECT * FROM attempts WHERE id=%s FOR UPDATE', (attempt_id,)).fetchone()
        reservation = conn.execute('SELECT *,expires_at<=clock_timestamp() AS expired FROM reservations WHERE attempt_id=%s FOR UPDATE', (attempt_id,)).fetchone()
        if reservation and reservation['state'] == 'ACTIVE' and reservation['expired']:
            release(conn, attempt, reservation, True)
            return True
    return False


def maintain():
    with transaction() as conn:
        expired = conn.execute("SELECT attempt_id FROM reservations WHERE state='ACTIVE' AND expires_at<=clock_timestamp() ORDER BY expires_at LIMIT 50").fetchall()
        due = conn.execute("SELECT attempt_id FROM payments WHERE state='PENDING' AND due_at<=clock_timestamp() ORDER BY due_at LIMIT 50").fetchall()
    for row in expired:
        expire_one(row['attempt_id'])
    for row in due:
        complete_payment(row['attempt_id'])
    with transaction() as conn:
        refunds = conn.execute("SELECT attempt_id FROM payments WHERE state='REFUND_PENDING' LIMIT 50").fetchall()
    for row in refunds:
        # Keep attempt-before-payment lock ordering even for refund-only transitions.
        with transaction() as conn:
            conn.execute('SELECT id FROM attempts WHERE id=%s FOR UPDATE', (row['attempt_id'],))
            changed = conn.execute("UPDATE payments SET state='REFUNDED',updated_at=clock_timestamp() WHERE attempt_id=%s AND state='REFUND_PENDING' RETURNING attempt_id", (row['attempt_id'],)).fetchone()
            if changed:
                conn.execute('UPDATE attempts SET version=version+1,updated_at=clock_timestamp() WHERE id=%s', (row['attempt_id'],))


def recover_delivery():
    with transaction() as conn:
        # Retained ledger repairs delivery even after a broker log/offset retention gap.
        conn.execute('''UPDATE outbox o SET published_at=NULL,next_at=clock_timestamp()
          WHERE published_at < clock_timestamp()-interval '60 seconds'
          AND ((kind='admitted' AND EXISTS (SELECT 1 FROM attempts a WHERE a.id=o.attempt_id AND NOT a.dispatched))
            OR (kind='confirmed' AND NOT EXISTS (SELECT 1 FROM fulfillments f WHERE f.attempt_id=o.attempt_id)))''')


def reconcile():
    with transaction() as conn:
        # One statement gives a consistent MVCC snapshot across all counters and rows.
        row = conn.execute('''SELECT i.*,
          (SELECT coalesce(sum(quantity),0) FROM reservations WHERE state='ACTIVE') AS active_units,
          (SELECT coalesce(sum(quantity),0) FROM attempts WHERE status='CONFIRMED') AS confirmed_units,
          (SELECT coalesce(sum(quantity),0) FROM reservations WHERE state='CONSUMED') AS consumed_units,
          (SELECT count(*) FROM attempts a LEFT JOIN reservations r ON a.id=r.attempt_id
             LEFT JOIN payments p ON a.id=p.attempt_id
             WHERE (a.status IN ('RESERVED','PAYMENT_PENDING') AND r.state IS DISTINCT FROM 'ACTIVE')
                OR (a.status='CONFIRMED' AND r.state IS DISTINCT FROM 'CONSUMED')
                OR (r.state='ACTIVE' AND a.status NOT IN ('RESERVED','PAYMENT_PENDING'))
                OR (r.state='CONSUMED' AND a.status<>'CONFIRMED')
                OR r.quantity<>a.quantity
                OR (a.status='CONFIRMED' AND p.state IS DISTINCT FROM 'SUCCEEDED')
                OR (p.state='SUCCEEDED' AND a.status<>'CONFIRMED')) AS inconsistent_orders,
          (SELECT count(*) FROM outbox WHERE published_at IS NULL) AS outbox_backlog,
          (SELECT count(*) FROM attempts WHERE status='QUEUED') AS queued,
          (SELECT count(*) FROM payments WHERE state='REFUND_PENDING') AS refunds_pending
          FROM sale_inventory i''').fetchone()
    row['healthy'] = (row['available'] + row['reserved'] + row['sold'] == 500
                      and row['reserved'] == row['active_units']
                      and row['sold'] == row['confirmed_units'] == row['consumed_units']
                      and row['inconsistent_orders'] == 0)
    return row
