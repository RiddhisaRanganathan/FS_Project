import json
import os
import time
import uuid

import pytest

pytest.importorskip('psycopg')
pytest.importorskip('confluent_kafka')
from confluent_kafka import Consumer, Producer
from app import config, service, worker


def test_outbox_publish_crash_redelivery(database, monkeypatch):
    if not os.getenv('TEST_KAFKA_BOOTSTRAP'):
        pytest.skip('Set TEST_KAFKA_BOOTSTRAP for a real broker test')
    monkeypatch.setattr(config, 'KAFKA_BOOTSTRAP', os.environ['TEST_KAFKA_BOOTSTRAP'])
    worker.ensure_topics()
    attempt_id = service.admit('broker-test', str(uuid.uuid4()), 'festival-sale', 'phone-001', 8)
    producer = Producer({'bootstrap.servers': config.KAFKA_BOOTSTRAP, 'enable.idempotence': True})
    with database.transaction() as conn:
        source = conn.execute('SELECT * FROM outbox').fetchone()
    event = {'eventId': source['id'], 'attemptId': attempt_id, 'kind': 'admitted'}
    # First publish succeeds, but its marker is deliberately not committed.
    worker.send(producer, worker.TOPIC, attempt_id, event)
    assert worker.publish_one(producer)
    consumer = Consumer({'bootstrap.servers': config.KAFKA_BOOTSTRAP, 'group.id': 'test-' + uuid.uuid4().hex,
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe([worker.TOPIC])
    received = 0
    deadline = time.monotonic() + 30
    try:
        while received < 2 and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if not message or message.error():
                continue
            data = json.loads(message.value())
            if data['attemptId'] != attempt_id:
                continue
            service.receive(data)
            consumer.commit(message=message, asynchronous=False)
            received += 1
    finally:
        consumer.close()
    assert received == 2
    assert service.allocate_next()
    assert not service.allocate_next()
    assert service.reconcile()['reserved'] == 8
    assert service.reconcile()['healthy']
