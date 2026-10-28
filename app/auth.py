import base64
import hashlib
import hmac
import json
import time

from app import config


def issue(buyer, ttl=86400):
    if len(config.AUTH_SECRET) < 32:
        raise ValueError('AUTH_SECRET must contain at least 32 characters')
    payload = base64.urlsafe_b64encode(json.dumps({'sub': buyer, 'exp': int(time.time()) + ttl}).encode()).decode().rstrip('=')
    signature = hmac.new(config.AUTH_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return payload + '.' + signature


def verify(token):
    try:
        payload, signature = token.split('.')
        expected = hmac.new(config.AUTH_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if len(config.AUTH_SECRET) < 32 or not hmac.compare_digest(signature, expected):
            raise ValueError()
        data = json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))
        if not isinstance(data['sub'], str) or not 1 <= len(data['sub']) <= 100 or data['exp'] <= time.time():
            raise ValueError()
        return data['sub']
    except (ValueError, KeyError, TypeError):
        raise ValueError('Invalid or expired access token') from None
