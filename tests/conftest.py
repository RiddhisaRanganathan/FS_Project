import os
import uuid

import pytest


@pytest.fixture
def database(monkeypatch):
    url = os.getenv('TEST_DATABASE_URL')
    if not url:
        pytest.skip('Set TEST_DATABASE_URL to run real PostgreSQL integration tests')
    import psycopg
    from psycopg import sql
    from app import config, db
    schema = 'test_' + uuid.uuid4().hex
    monkeypatch.setattr(config, 'DATABASE_URL', url)
    monkeypatch.setattr(config, 'ADMISSION_PAUSED', False)
    monkeypatch.setattr(config, 'RESERVATIONS_PAUSED', False)
    monkeypatch.setattr(config, 'RECONCILIATION_ONLY', False)
    monkeypatch.setattr(config, 'MAX_PENDING', 100000)
    monkeypatch.setattr(db, 'schema', schema)
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    try:
        db.migrate()
        yield db
    finally:
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
