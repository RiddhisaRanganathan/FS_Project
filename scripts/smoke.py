"""Exercise the running Compose stack without bypassing Kafka or workers."""
import argparse
import json
import sys
import time
import urllib.request
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.auth import issue


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://localhost:8000')
    parser.add_argument('--quantity', type=int, default=3)
    args = parser.parse_args()
    token = issue('smoke-' + uuid.uuid4().hex)
    def request(path, body=None, key=None):
        headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
        if key:
            headers['Idempotency-Key'] = key
        req = urllib.request.Request(args.url + path, headers=headers,
                                     data=json.dumps(body).encode() if body is not None else None)
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.load(response)
    payload = {'saleId': 'festival-sale', 'sku': 'phone-001', 'quantity': args.quantity}
    key = str(uuid.uuid4())
    admitted = request('/api/checkout-attempts', payload, key)
    assert request('/api/checkout-attempts', payload, key)['attemptId'] == admitted['attemptId']
    path = '/api/checkout-attempts/' + admitted['attemptId']
    paid = False
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        data = request(path)
        print(data['status'])
        if data['status'] == 'RESERVED' and not paid:
            request(path + '/payment', {'outcome': 'success', 'delaySeconds': 1})
            paid = True
        if data['status'] == 'CONFIRMED':
            assert data['quantity'] == args.quantity
            print('PASS: durable admission, duplicate retry, Kafka allocation, payment, confirmation')
            return
        if data['status'] in ('FAILED', 'SOLD_OUT'):
            raise RuntimeError(f'Checkout reached {data["status"]}: {data.get("reason")}')
        time.sleep(1)
    raise TimeoutError('Checkout did not confirm within 60 seconds')


if __name__ == '__main__':
    main()
