from contextlib import contextmanager
from pathlib import Path
from threading import BoundedSemaphore, Lock

import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from psycopg_pool import ConnectionPool, PoolTimeout, TooManyRequests

from app import config

slots = BoundedSemaphore(config.DB_POOL_SIZE)
schema = 'public'
_pool = None
_pool_url = None
_pool_lock = Lock()


def pool():
    global _pool, _pool_url
    with _pool_lock:
        if _pool is None or _pool_url != config.DATABASE_URL:
            if _pool is not None:
                _pool.close()
            _pool = ConnectionPool(config.DATABASE_URL, min_size=1, max_size=config.DB_POOL_SIZE,
                                   max_waiting=config.MAX_INFLIGHT, timeout=2,
                                   kwargs={'row_factory': dict_row, 'connect_timeout': 3,
                                           'options': '-c statement_timeout=5000 -c lock_timeout=2000'},
                                   check=ConnectionPool.check_connection, open=True)
            _pool_url = config.DATABASE_URL
        return _pool


@contextmanager
def transaction():
    if not slots.acquire(timeout=2):
        raise TimeoutError('Database concurrency limit reached')
    try:
        try:
            with pool().connection() as conn:
                conn.execute(sql.SQL('SET LOCAL search_path TO {}').format(sql.Identifier(schema)))
                yield conn
        except (PoolTimeout, TooManyRequests) as error:
            raise TimeoutError('Database pool is at capacity') from error
    finally:
        slots.release()


def migrate():
    with transaction() as conn:
        conn.execute('SELECT pg_advisory_xact_lock(100)')
        conn.execute('CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY)')
        for path in sorted(Path('migrations').glob('*.sql')):
            if not conn.execute('SELECT 1 FROM schema_migrations WHERE name=%s', (path.name,)).fetchone():
                conn.execute(path.read_text())
                conn.execute('INSERT INTO schema_migrations VALUES (%s)', (path.name,))
