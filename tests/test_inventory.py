import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

pytest.importorskip('psycopg')
from app import config, service


def create(quantity, buyer='customer', key=None):
    return service.admit(buyer, key or str(uuid.uuid4()), 'festival-sale', 'phone-001', quantity)


def dispatch(database, attempt_id):
    with database.transaction() as conn:
        event = conn.execute("SELECT * FROM outbox WHERE attempt_id=%s AND kind='admitted'", (attempt_id,)).fetchone()
    service.receive({'eventId': event['id'], 'attemptId': attempt_id, 'kind': event['kind']})


def expire(database, attempt_id):
    with database.transaction() as conn:
        conn.execute("UPDATE reservations SET expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=%s", (attempt_id,))


def test_atomic_concurrent_multi_unit_and_duplicates(database):
    attempts = [create(7, f'customer-{index}') for index in range(100)]
    for attempt_id in attempts:
        dispatch(database, attempt_id)
        dispatch(database, attempt_id)
    def drain(_):
        for _ in range(150):
            service.allocate_next()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(drain, range(8)))
    report = service.reconcile()
    assert report['healthy']
    assert report['reserved'] == 497
    assert report['available'] == 3
    assert report['sold'] == 0
    # Remaining attempts wait while allocated units can still be released.
    assert report['queued'] == 29


def test_duplicate_admission_and_payload_conflict(database):
    key = str(uuid.uuid4())
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: create(4, key=key), range(12)))
    assert len(set(results)) == 1
    with pytest.raises(service.Conflict):
        create(5, key=key)
    with database.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM outbox').fetchone()['n'] == 1


def test_confirmation_and_fulfillment_are_idempotent(database):
    attempt_id = create(12)
    dispatch(database, attempt_id)
    assert service.allocate_next()
    provider_id = service.start_payment(attempt_id, 'customer', 'success', 0)
    assert service.start_payment(attempt_id, 'customer', 'success', 0) == provider_id
    with ThreadPoolExecutor(max_workers=6) as pool:
        assert sum(pool.map(lambda _: service.complete_payment(attempt_id), range(10))) == 1
    assert service.status(attempt_id, 'customer')['status'] == 'CONFIRMED'
    assert not service.expire_one(attempt_id)
    with database.transaction() as conn:
        event = conn.execute("SELECT * FROM outbox WHERE kind='confirmed'").fetchone()
    for _ in range(3):
        service.receive({'eventId': event['id'], 'attemptId': attempt_id, 'kind': 'confirmed'})
    with database.transaction() as conn:
        assert conn.execute('SELECT count(*) AS n FROM fulfillments').fetchone()['n'] == 1
    assert service.reconcile()['sold'] == 12
    assert service.reconcile()['healthy']


@pytest.mark.parametrize('outcome', ['success', 'failure'])
def test_expiry_racing_late_payment_never_confirms(database, outcome):
    attempt_id = create(9)
    dispatch(database, attempt_id)
    service.allocate_next()
    service.start_payment(attempt_id, 'customer', outcome, 0)
    expire(database, attempt_id)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda fn: fn(attempt_id), [service.complete_payment, service.expire_one]))
    assert service.status(attempt_id, 'customer')['status'] == 'FAILED'
    assert service.reconcile()['available'] == 500
    version_before_refund = service.status(attempt_id, 'customer')['version']
    service.maintain()
    assert service.status(attempt_id, 'customer')['payment_state'] == ('REFUNDED' if outcome == 'success' else 'FAILED')
    if outcome == 'success':
        assert service.status(attempt_id, 'customer')['version'] > version_before_refund
    assert service.reconcile()['healthy']


def test_payment_failure_releases_to_next_fifo_request(database):
    first, second = create(500), create(3)
    for attempt_id in (first, second):
        dispatch(database, attempt_id)
    assert service.allocate_next()
    assert not service.allocate_next()
    service.start_payment(first, 'customer', 'failure', 0)
    service.complete_payment(first)
    assert service.allocate_next()
    assert service.status(second, 'customer')['status'] == 'RESERVED'
    assert service.reconcile()['reserved'] == 3
    assert service.reconcile()['healthy']


def test_sold_out_only_after_irrevocable_stock_consumption(database):
    first, second = create(500), create(1)
    for attempt_id in (first, second):
        dispatch(database, attempt_id)
    service.allocate_next()
    service.start_payment(first, 'customer', 'success', 0)
    service.complete_payment(first)
    assert service.allocate_next()
    assert service.status(second, 'customer')['status'] == 'SOLD_OUT'
    assert service.reconcile()['sold'] == 500


def test_failed_transaction_rolls_back_inventory(database):
    with pytest.raises(RuntimeError):
        with database.transaction() as conn:
            conn.execute('UPDATE sale_inventory SET available=available-5,reserved=reserved+5')
            raise RuntimeError('Injected worker crash before commit')
    assert service.reconcile()['available'] == 500
    assert service.reconcile()['healthy']


def test_admission_bound_preserves_duplicate_recovery(database, monkeypatch):
    monkeypatch.setattr(config, 'MAX_PENDING', 1)
    key = str(uuid.uuid4())
    first = create(2, key=key)
    assert create(2, key=key) == first
    with pytest.raises(service.Overloaded):
        create(1)


def test_fifo_waits_for_oldest_delivery(database):
    first, second = create(4), create(2)
    dispatch(database, second)
    assert not service.allocate_next()
    dispatch(database, first)
    assert service.allocate_next()
    assert service.status(first, 'customer')['status'] == 'RESERVED'


def test_delivery_reconciliation_republishes_missing_broker_event(database):
    attempt_id = create(4)
    with database.transaction() as conn:
        conn.execute("UPDATE outbox SET published_at=clock_timestamp()-interval '2 minutes'")
    service.recover_delivery()
    with database.transaction() as conn:
        assert conn.execute('SELECT published_at FROM outbox').fetchone()['published_at'] is None
    dispatch(database, attempt_id)
    assert service.allocate_next()


def test_reconciliation_detects_balanced_counter_corruption(database):
    with database.transaction() as conn:
        conn.execute('UPDATE sale_inventory SET available=499,reserved=1')
    assert not service.reconcile()['healthy']
