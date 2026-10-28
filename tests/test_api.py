import os
import uuid

import pytest

pytest.importorskip('fastapi')
pytest.importorskip('psycopg')
from fastapi.testclient import TestClient
from app import auth, config, service
from app.api import app
import app.api as api_module


@pytest.fixture
def client(database, monkeypatch):
    if not os.getenv('TEST_REDIS_URL'):
        pytest.skip('Set TEST_REDIS_URL for API integration tests')
    import redis
    monkeypatch.setattr(api_module, 'cache', redis.Redis.from_url(os.environ['TEST_REDIS_URL'], decode_responses=True))
    monkeypatch.setattr(config, 'AUTH_SECRET', 'test-customer-secret-at-least-32-characters')
    monkeypatch.setattr(config, 'ADMIN_SECRET', 'test-operator-secret-at-least-32-characters')
    with TestClient(app) as client:
        yield client


def headers(buyer='api-customer'):
    return {'Authorization': 'Bearer ' + auth.issue(buyer), 'Idempotency-Key': str(uuid.uuid4())}


def body(quantity=3):
    return {'saleId': 'festival-sale', 'sku': 'phone-001', 'quantity': quantity}


def test_end_to_end_http_checkout(client, database):
    credentials = headers()
    response = client.post('/api/checkout-attempts', headers=credentials, json=body())
    assert response.status_code == 202
    attempt_id = response.json()['attemptId']
    assert not response.json()['purchaseConfirmed']
    assert client.post('/api/checkout-attempts', headers=credentials, json=body()).json()['attemptId'] == attempt_id
    assert client.post('/api/checkout-attempts', headers=credentials, json=body(4)).status_code == 409
    recovery = client.get('/api/checkout-attempts/by-key/' + credentials['Idempotency-Key'], headers=credentials)
    assert recovery.json()['id'] == attempt_id
    with database.transaction() as conn:
        event = conn.execute('SELECT * FROM outbox WHERE attempt_id=%s', (attempt_id,)).fetchone()
    service.receive({'eventId': event['id'], 'attemptId': attempt_id, 'kind': 'admitted'})
    service.allocate_next()
    paid = client.post(f'/api/checkout-attempts/{attempt_id}/payment', headers=credentials,
                       json={'outcome': 'success', 'delaySeconds': 0})
    assert paid.status_code == 200
    callback_headers = {'X-Admin-Secret': config.ADMIN_SECRET}
    callback_path = f'/api/simulator/callback/{attempt_id}'
    assert client.post(callback_path, headers=callback_headers).json()['changed']
    assert not client.post(callback_path, headers=callback_headers).json()['changed']
    status = client.get(f'/api/checkout-attempts/{attempt_id}', headers=credentials)
    assert status.json()['status'] == 'CONFIRMED'
    conditional = client.get(f'/api/checkout-attempts/{attempt_id}', headers={**credentials, 'If-None-Match': status.headers['etag']})
    assert conditional.status_code == 304
    assert client.get(f'/api/checkout-attempts/{attempt_id}', headers=headers('another-customer')).status_code == 404
    assert service.reconcile()['healthy']


@pytest.mark.parametrize('quantity', [0, -1, 501, 1.5, '2', True])
def test_quantity_validation(client, quantity):
    assert client.post('/api/checkout-attempts', headers=headers(), json=body(quantity)).status_code == 422


def test_authentication_and_request_bounds(client):
    assert client.post('/api/checkout-attempts', json=body()).status_code == 401
    assert client.post('/api/checkout-attempts', headers=headers(), content=b'x' * 4097).status_code == 413
    assert client.post('/api/simulator/callback/' + str(uuid.uuid4())).status_code == 403


def test_redis_outage_fails_admission_safely(client, monkeypatch):
    import redis
    unavailable = redis.Redis(host='127.0.0.1', port=1, socket_connect_timeout=0.1)
    monkeypatch.setattr(api_module, 'cache', unavailable)
    assert client.post('/api/checkout-attempts', headers=headers(), json=body()).status_code == 503
    assert service.reconcile()['queued'] == 0
