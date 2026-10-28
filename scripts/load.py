"""Open-loop admission load generator with a bounded client queue."""
import argparse
import json
import math
import os
import queue
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.auth import issue


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values) - 1, math.ceil(len(values) * fraction) - 1)] * 1000, 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://localhost:8000')
    parser.add_argument('--requests', type=int, default=100000)
    parser.add_argument('--duration', type=float, default=10)
    parser.add_argument('--concurrency', type=int, default=256)
    parser.add_argument('--queue-capacity', type=int, default=20000)
    parser.add_argument('--quantity', type=int, default=1)
    parser.add_argument('--output', default='test-results/load.json')
    args = parser.parse_args()
    if min(args.requests, args.duration, args.concurrency, args.queue_capacity, args.quantity) <= 0 or args.quantity > 500:
        parser.error('Use positive capacity/timing values and quantity between 1 and 500')
    if len(os.getenv('AUTH_SECRET', '')) < 32:
        parser.error('Set AUTH_SECRET to the same secret as the local API')
    jobs = queue.Queue(maxsize=args.queue_capacity)
    lock = threading.Lock()
    statuses, latencies, schedule_lags, uncertain = Counter(), [], [], []
    run_id = uuid.uuid4().hex
    def worker():
        while True:
            job = jobs.get()
            if job is None:
                jobs.task_done()
                break
            index, due = job
            key = str(uuid.uuid4())
            buyer = f'load-{run_id}-{index}'
            req = urllib.request.Request(args.url + '/api/checkout-attempts', method='POST',
                  data=json.dumps({'saleId': 'festival-sale', 'sku': 'phone-001', 'quantity': args.quantity}).encode(),
                  headers={'Content-Type': 'application/json', 'Idempotency-Key': key, 'Authorization': 'Bearer ' + issue(buyer)})
            sent = time.monotonic()
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    response.read()
                    status = str(response.status)
            except urllib.error.HTTPError as error:
                status = str(error.code)
                error.close()
            except (OSError, TimeoutError):
                status = 'uncertain'
            finished = time.monotonic()
            with lock:
                statuses[status] += 1
                latencies.append(finished - sent)
                schedule_lags.append(sent - due)
                if status == 'uncertain':
                    uncertain.append({'buyer': buyer, 'idempotencyKey': key})
            jobs.task_done()
    threads = [threading.Thread(target=worker) for _ in range(args.concurrency)]
    for thread in threads:
        thread.start()
    start = time.monotonic()
    dropped = 0
    for index in range(args.requests):
        due = start + args.duration * index / args.requests
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        try:
            jobs.put_nowait((index, due))
        except queue.Full:
            dropped += 1
    dispatch_seconds = time.monotonic() - start
    jobs.join()
    for _ in threads:
        jobs.put(None)
    for thread in threads:
        thread.join()
    report = {'requested': args.requests, 'scheduled_duration_seconds': args.duration,
              'dispatch_seconds': round(dispatch_seconds, 3), 'completed_seconds': round(time.monotonic() - start, 3),
              'client_dropped': dropped, 'http_statuses': dict(statuses),
              'http_latency_ms': {str(p): percentile(latencies, p / 100) for p in (50, 95, 99)},
              'scheduled_to_send_lag_ms': {str(p): percentile(schedule_lags, p / 100) for p in (50, 95, 99)},
              'uncertain_attempts': uncertain,
              'scope': 'Admission only. Measure inventory correctness, drain time, and polling separately.'}
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2))
    print(json.dumps({key: value for key, value in report.items() if key != 'uncertain_attempts'}, indent=2))


if __name__ == '__main__':
    main()
