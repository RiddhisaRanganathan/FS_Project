import os


DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://sale:sale@localhost:5432/sale')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
AUTH_SECRET = os.getenv('AUTH_SECRET', '')
ADMIN_SECRET = os.getenv('ADMIN_SECRET', '')
RESERVATION_SECONDS = int(os.getenv('RESERVATION_SECONDS', '300'))
MAX_PENDING = int(os.getenv('MAX_PENDING', '100000'))
MAX_INFLIGHT = int(os.getenv('MAX_INFLIGHT', '64'))
DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '12'))
ADMISSION_PAUSED = os.getenv('ADMISSION_PAUSED', 'false').lower() == 'true'
RESERVATIONS_PAUSED = os.getenv('RESERVATIONS_PAUSED', 'false').lower() == 'true'
RECONCILIATION_ONLY = os.getenv('RECONCILIATION_ONLY', 'false').lower() == 'true'
if min(RESERVATION_SECONDS, MAX_PENDING, MAX_INFLIGHT, DB_POOL_SIZE) < 1:
    raise ValueError('Capacity and timing settings must be positive')
