"""Connection + transaction helper for the accounting core.

Wraps the existing mysql.connector pool from database.py so we don't
introduce a second connection mechanism. Adds:

- A `tx()` context manager that gives an autocommit-off connection,
  commits on clean exit, rolls back on any exception.
- An `OrgScopedCursor` that injects the active organization_id into
  named-parameter dicts under the key `org_id`. Queries against
  tenant-scoped tables MUST include `WHERE organization_id = %(org_id)s`.

Why this and not SQLAlchemy: per the project decision, ORM lands after
Phase E. Until then, every core query is hand-rolled SQL using
mysql.connector parameterized statements. The static test in
tests/test_static_org_scope.py enforces the org_id predicate at build
time — see that test for the enforcement contract.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import mysql.connector  # type: ignore[import-untyped]
from mysql.connector.pooling import PooledMySQLConnection  # type: ignore[import-untyped]

# database.py is the existing file at /home/srpc/srpc_api/database.py.
# It exposes get_connection() returning a PooledMySQLConnection.
from database import get_connection as _get_connection

from .tenancy import get_active_org_id


@contextmanager
def tx() -> Iterator[PooledMySQLConnection]:
    """Yield a connection inside an explicit transaction.

    On clean exit: commits. On any exception: rolls back and re-raises.
    Connection is always returned to the pool.

    Usage::

        with tx() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("...", {"org_id": get_active_org_id(), ...})
    """
    conn = _get_connection()
    try:
        # mysql.connector connections from a pool default to autocommit=False
        # but be explicit; some configs flip this.
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
