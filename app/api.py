import asyncio
import json
import logging
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore

import psycopg
import redis
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from app import auth, config, service
from app.db import transaction

log = logging.getLogger('sale.api')
cache = redis.Redis.from_url(config.REDIS_URL, socket_connect_timeout=0.15, socket_timeout=0.15, decode_responses=True)
inflight = BoundedSemaphore(config.MAX_INFLIGHT)
fallback = BoundedSemaphore(4)
CACHE_SET = '''local old=redis.call('GET',KEYS[1]);
if not old or tonumber(cjson.decode(old).version)<=tonumber(ARGV[2]) then
 redis.call('SET',KEYS[1],ARGV[1],'PX',2000); return 1 end; return 0'''
RATE_LIMIT = '''local n=redis.call('INCR',KEYS[1]);
if n==1 then redis.call('EXPIRE',KEYS[1],ARGV[1]) end; return n'''


@asynccontextmanager
async def lifespan(app):
    if len(config.AUTH_SECRET) < 32 or len(config.ADMIN_SECRET) < 32:
        raise RuntimeError('AUTH_SECRET and ADMIN_SECRET must each contain at least 32 characters')
    yield


app = FastAPI(title='Festival Flash Sale', lifespan=lifespan)


@app.middleware('http')
async def bounds(request: Request, call_next):
    request_id = secrets.token_hex(8)
    started = time.monotonic()
    if not inflight.acquire(blocking=False):
        return JSONResponse({'detail': 'Server is at capacity'}, status_code=503, headers={'Retry-After': '3'})
    try:
        # Read a bounded stream before parsing; chunked bodies obey the same limit.
        body = bytearray()
        async with asyncio.timeout(5):
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 4096:
                    return JSONResponse({'detail': 'Request body exceeds 4 KB'}, status_code=413)
        request._body = bytes(body)
        response = await call_next(request)
        response.headers.update({'X-Request-ID': request_id, 'X-Content-Type-Options': 'nosniff',
                                 'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store',
                                 'Content-Security-Policy': "default-src 'self'; style-src 'self'; script-src 'self'; frame-ancestors 'none'; base-uri 'none'"})
        return response
    except TimeoutError:
        return JSONResponse({'detail': 'Request deadline exceeded; retry with the same key'}, status_code=503)
    finally:
        inflight.release()
        log.info('http request_id=%s method=%s path=%s duration_ms=%.1f', request_id, request.method, request.url.path, (time.monotonic()-started)*1000)


@app.exception_handler(psycopg.Error)
@app.exception_handler(TimeoutError)
def dependency_error(request, error):
    log.error('database_unavailable type=%s', type(error).__name__)
    return JSONResponse({'detail': 'Database unavailable; recover using the same idempotency key'}, status_code=503, headers={'Retry-After': '3'})


@app.exception_handler(service.Conflict)
def conflict(request, error):
    return JSONResponse({'detail': str(error)}, status_code=409)


@app.exception_handler(service.Overloaded)
def overloaded(request, error):
    return JSONResponse({'detail': str(error)}, status_code=503, headers={'Retry-After': '3'})


def buyer(authorization: str = Header(default='')):
    if not authorization.startswith('Bearer '):
        raise HTTPException(401, 'A signed demo access token is required')
    try:
        return auth.verify(authorization[7:])
    except ValueError as error:
        raise HTTPException(401, str(error)) from None


def admin(x_admin_secret: str = Header(default='')):
    if len(config.ADMIN_SECRET) < 32 or not secrets.compare_digest(x_admin_secret, config.ADMIN_SECRET):
        raise HTTPException(403, 'Operator authorization required')


def rate_limit(identity, limit, seconds):
    try:
        count = cache.eval(RATE_LIMIT, 1, f'rate:{identity}:{int(time.time())//seconds}', seconds)
    except redis.RedisError:
        raise HTTPException(503, 'Admission limiter unavailable', headers={'Retry-After': '3'}) from None
    if count > limit:
        raise HTTPException(429, 'Request rate exceeded', headers={'Retry-After': str(seconds)})


class Checkout(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    saleId: str = Field(pattern='^festival-sale$')
    sku: str = Field(pattern='^phone-001$')
    quantity: int = Field(ge=1, le=500)


class Payment(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    outcome: str = Field(pattern='^(success|failure)$')
    delaySeconds: int = Field(default=2, ge=0, le=900)


@app.get('/api/product')
def product():
    return {'saleId': 'festival-sale', 'sku': 'phone-001', 'name': 'Orbit One', 'price': 39900,
            'currency': 'INR', 'totalStock': 500, 'reservationSeconds': config.RESERVATION_SECONDS}


@app.post('/api/checkout-attempts', status_code=202)
def checkout(body: Checkout, response: Response, identity=Depends(buyer), idempotency_key: str = Header(min_length=16, max_length=100)):
    rate_limit('checkout:' + identity, 20, 60)
    attempt_id = service.admit(identity, idempotency_key, body.saleId, body.sku, body.quantity)
    current = service.status(attempt_id, identity)
    response.headers.update({'Location': f'/api/checkout-attempts/{attempt_id}', 'Retry-After': '3'})
    return {'attemptId': attempt_id, 'statusUrl': f'/api/checkout-attempts/{attempt_id}',
            'status': current['status'], 'purchaseConfirmed': current['status'] == 'CONFIRMED'}


@app.get('/api/checkout-attempts/by-key/{key}')
def recover(key: str, identity=Depends(buyer)):
    rate_limit('recovery:' + identity, 30, 60)
    with transaction() as conn:
        row = conn.execute('SELECT id FROM attempts WHERE buyer=%s AND idempotency_key=%s', (identity, key)).fetchone()
    if not row:
        raise HTTPException(404, 'Attempt not found; retry the same checkout key')
    return service.status(row['id'], identity)


@app.get('/api/checkout-attempts/{attempt_id}')
def get_status(attempt_id: uuid.UUID, response: Response, request: Request, identity=Depends(buyer)):
    cache_key = f'status:{identity}:{attempt_id}'
    data = None
    try:
        raw = cache.get(cache_key)
        if raw:
            data = json.loads(raw)
    except redis.RedisError:
        pass
    if data is None:
        if not fallback.acquire(blocking=False):
            raise HTTPException(503, 'Status service is busy', headers={'Retry-After': '5'})
        try:
            data = service.status(attempt_id, identity)
            if data is None:
                raise HTTPException(404, 'Attempt not found')
            data = jsonable_encoder(data)
            try:
                cache.eval(CACHE_SET, 1, cache_key, json.dumps(data), data['version'])
            except redis.RedisError:
                pass
        finally:
            fallback.release()
    etag = f'"{data["version"]}"'
    data['server_time'] = datetime.now(timezone.utc).isoformat()
    headers = {'ETag': etag, 'Retry-After': '3'}
    if request.headers.get('if-none-match') == etag:
        return Response(status_code=304, headers=headers)
    response.headers.update(headers)
    return data


@app.post('/api/checkout-attempts/{attempt_id}/payment')
def payment(attempt_id: uuid.UUID, body: Payment, identity=Depends(buyer)):
    rate_limit('payment:' + identity, 30, 60)
    payment_id = service.start_payment(attempt_id, identity, body.outcome, body.delaySeconds)
    if payment_id is None:
        raise HTTPException(404, 'Attempt not found')
    return {'providerId': payment_id, 'status': 'PAYMENT_PENDING'}


@app.post('/api/simulator/callback/{attempt_id}', dependencies=[Depends(admin)])
def callback(attempt_id: uuid.UUID):
    # The provider's durable result is read internally; caller data cannot declare success.
    return {'changed': service.complete_payment(attempt_id)}


@app.get('/health/live')
def live():
    return {'status': 'live'}


@app.get('/health/ready')
def ready():
    with transaction() as conn:
        conn.execute('SELECT 1 FROM sale_inventory')
    try:
        cache.ping()
    except redis.RedisError:
        raise HTTPException(503, 'Redis admission limiter unavailable') from None
    return {'status': 'ready', 'broker': 'outbox buffers broker outages'}


@app.get('/api/ops/report', dependencies=[Depends(admin)])
def report():
    return service.reconcile()


@app.get('/')
def index():
    return FileResponse(Path('web/index.html'))


app.mount('/static', StaticFiles(directory='web'), name='static')
