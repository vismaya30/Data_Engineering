from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection

from src.common.config import Settings


@contextmanager
def connect(settings: Settings) -> Iterator[connection]:
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
