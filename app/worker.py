import json
import logging
import random
import signal
import threading
import time

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from app import config, service
from app.db import transaction

log = logging.getLogger('sale.worker')
stop = threading.Event()
TOPIC = 'sale.events'
DLQ = 'sale.dead-letter'


def ensure_topics():
    admin = AdminClient({'bootstrap.servers': config.KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=5).topics
    topics = [NewTopic(name, num_partitions=3, replication_factor=1,
                       config={'retention.ms': '604800000', 'retention.bytes': '1073741824'})
              for name in (TOPIC, DLQ) if name not in existing]
    if not topics:
        return
    for future in admin.create_topics(topics).values():
        future.result(timeout=10)


def send(producer, topic, key, payload):
    result = []
    producer.produce(topic, key=str(key), value=json.dumps(payload),
                     on_delivery=lambda error, message: result.append(error))
    deadline = time.monotonic() + 12
    while not result and time.monotonic() < deadline and not stop.is_set():
        producer.poll(0.1)
    if not result or result[0]:
        raise RuntimeError(f'Kafka delivery uncertain: {result}')


def publish_one(producer):
    with transaction() as conn:
        row = conn.execute('''SELECT * FROM outbox WHERE published_at IS NULL AND next_at<=clock_timestamp()
                              ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED''').fetchone()
        if not row:
            return False
        try:
            send(producer, TOPIC, row['attempt_id'], {'eventId': row['id'], 'kind': row['kind'], 'attemptId': str(row['attempt_id'])})
        except Exception as error:
            delay = min(60, 2 ** min(row['failures'], 6)) + random.random()
            conn.execute("UPDATE outbox SET failures=failures+1,last_error=%s,next_at=clock_timestamp()+%s * interval '1 second' WHERE id=%s",
                         (str(error)[:500], delay, row['id']))
            log.warning('outbox_retry event=%s error=%s', row['id'], error)
        else:
            conn.execute('UPDATE outbox SET published_at=clock_timestamp(),last_error=NULL WHERE id=%s', (row['id'],))
    return True


def publisher():
    producer = Producer({'bootstrap.servers': config.KAFKA_BOOTSTRAP,
                         'enable.idempotence': True, 'acks': 'all',
                         'message.timeout.ms': 10000, 'queue.buffering.max.messages': 100})
    while not stop.is_set():
        try:
            if not publish_one(producer):
                stop.wait(0.2)
        except Exception:
            log.exception('publisher_failed')
            stop.wait(1)
    producer.flush(2)


def consumer():
    while not stop.is_set():
        try:
            ensure_topics()
            break
        except Exception:
            log.exception('broker_not_ready')
            stop.wait(3)
    if stop.is_set():
        return
    client = Consumer({'bootstrap.servers': config.KAFKA_BOOTSTRAP, 'group.id': 'sale-durable-v1',
                       'enable.auto.commit': False, 'auto.offset.reset': 'earliest',
                       'queued.max.messages.kbytes': 1024, 'max.poll.interval.ms': 300000})
    producer = Producer({'bootstrap.servers': config.KAFKA_BOOTSTRAP, 'enable.idempotence': True,
                         'acks': 'all', 'message.timeout.ms': 10000})
    client.subscribe([TOPIC])
    try:
        while not stop.is_set():
            message = client.poll(0.5)
            if message is None:
                continue
            if message.error():
                log.warning('consumer_poll error=%s', message.error())
                stop.wait(1)
                continue
            failures = 0
            while not stop.is_set():
                try:
                    event = json.loads(message.value())
                    service.receive(event)
                except (ValueError, KeyError, TypeError) as error:
                    try:
                        payload = message.value().decode(errors='replace')
                        with transaction() as conn:
                            conn.execute('INSERT INTO dead_letters(topic,partition_id,offset_id,payload,reason) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                                         (message.topic(), message.partition(), message.offset(), payload, str(error)))
                        send(producer, DLQ, message.key(), {'payload': payload, 'reason': str(error),
                                                           'partition': message.partition(), 'offset': message.offset()})
                    except Exception:
                        log.exception('dead_letter_delivery_failed')
                        stop.wait(1)
                        continue
                except Exception:
                    failures += 1
                    log.exception('consumer_retry partition=%s offset=%s failures=%s', message.partition(), message.offset(), failures)
                    stop.wait(min(30, 2 ** min(failures, 5)) + random.random())
                    # Infrastructure errors stay recoverable and never become business failures.
                    continue
                try:
                    client.commit(message=message, asynchronous=False)
                except KafkaException:
                    log.exception('offset_commit_failed')
                break
    finally:
        client.close()


def run():
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    threads = []
    if not config.RECONCILIATION_ONLY:
        threads = [threading.Thread(target=target, name=target.__name__) for target in (publisher, consumer)]
        for thread in threads:
            thread.start()
    last_reconcile = 0
    healthy = False
    while not stop.is_set():
        try:
            if time.monotonic() - last_reconcile >= 5:
                report = service.reconcile()
                healthy = report['healthy']
                log.log(logging.INFO if healthy else logging.CRITICAL, 'reconciliation %s', json.dumps(report))
                last_reconcile = time.monotonic()
                if not config.RECONCILIATION_ONLY:
                    service.recover_delivery()
            if healthy and not config.RECONCILIATION_ONLY:
                service.maintain()
                for _ in range(20):
                    if stop.is_set() or not service.allocate_next():
                        break
        except Exception:
            log.exception('scheduler_failed')
        stop.wait(0.2)
    for thread in threads:
        thread.join(timeout=15)
