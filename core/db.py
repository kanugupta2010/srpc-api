"""Connection + transaction helper for the accounting core.

Owns its own mysql.connector connection lifecycle — intentionally does NOT
share with the existing database.py module. Reasoning:

    1. The loyalty program's database.py exposes a FastAPI-dependency-style
       generator (`yield conn`), which doesn't compose cleanly with our
       context-manager-based transaction boundary.
    2. Keeping the core domain's DB access isolated means future work
       (SQLAlchemy migration in Phase E, connection pool tuning, etc.)
       can proceed without touching the loyalty codepath.
    3. Both modules read from the same environment variables, so they
       connect to the same database — no duplication of config.

What's here:
    * `tx()` — context manager yielding a connection in an explicit
      transaction. Commits on clean exit, rolls back on any exception,
      always closes (returns to the pool).
    * `org_params(**extra)` — parameter-dict builder that always injects
      the active organization_id under key `org_id`.

A module-level connection pool is lazily created on first use. The pool
size is intentionally small (5) because the accounting core is
transactional and short-lived; the loyalty program has its own pool of
10. Total pool usage stays well below MySQL's default max_connections.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import mysql.connector  # type: ignore[import-untyped]
from mysql.connector.pooling import (  # type: ignore[import-untyped]
    MySQLConnectionPool,
    PooledMySQLConnection,
)

from .tenancy import get_active_org_id

_POOL: Optional[MySQLConnectionPool] = None


def _get_pool() -> MySQLConnectionPool:
    """Lazily create the accounting-core connection pool."""
    global _POOL
    if _POOL is None:
        _POOL = MySQLConnectionPool(
            pool_name="srpc_core_pool",
            pool_size=5,
            pool_reset_session=True,
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", "3306")),
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_NAME"],
            autocommit=False,
            use_pure=True,
            charset="utf8mb4",
            collation="utf8mb4_unicode_ci",
        )
    return _POOL


def _acquire() -> PooledMySQLConnection:
    """Acquire a connection from the pool, ensuring it is live."""
    conn = _get_pool().get_connection()
    # Pooled connections can go stale (RDS idle-close); ping forces a
    # reconnect attempt if needed. reconnect=True, attempts=1, delay=0s.
    try:
        conn.ping(reconnect=True, attempts=1, delay=0)
    except mysql.connector.Error:
        conn.close()
        raise
    return conn


@contextmanager
def tx() -> Iterator[PooledMySQLConnection]:
    """Yield a connection inside an explicit transaction.

    On clean exit: commits. On any exception: rolls back and re-raises.
    Connection is always returned to the pool.

    Usage::

        from core.tenancy import bind_org
        from core.db import tx

        with bind_org(org_id), tx() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("...", {"org_id": get_active_org_id(), ...})
    """
    conn = _acquire()
    try:
        # Defensive: pool sets autocommit=False, but some pool resets
        # have toggled it. Make it explicit.
        conn.autocommit = False
        conn.start_transaction()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()  # returns to pool


@contextmanager
def cursor(conn: PooledMySQLConnection, *, dictionary: bool = True):
    """Helper to manage a cursor lifecycle inside a tx() block."""
    cur = conn.cursor(dictionary=dictionary)
    try:
        yield cur
    finally:
        cur.close()


def org_params(**extra: Any) -> dict[str, Any]:
    """Return a parameter dict pre-populated with the active org_id.

    Use this everywhere instead of building dicts by hand — it is the
    single chokepoint where org_id enters a query, which makes the
    static test reliable. Example::

        cur.execute(
            "SELECT id FROM ledgers WHERE organization_id = %(org_id)s "
            "AND name = %(name)s",
            org_params(name="Cash-in-Hand"),
        )
    """
    return {"org_id": get_active_org_id(), **extra}