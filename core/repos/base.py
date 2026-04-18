"""Repository base for org-scoped data access in the accounting core.

Provides one helper: `OrgScopedRepository` — a thin convention wrapper
that binds a connection and exposes a `params(**extra)` method to build
parameter dicts that always include `org_id`. There is no metaclass
magic; the discipline is enforced by tests/test_static_org_scope.py.

Repositories deliberately do NOT cache or hold connections across
methods. Each call expects the caller (a service inside a tx() block)
to pass the connection in. This keeps transaction boundaries explicit
and visible at the service layer.
"""
from __future__ import annotations

from typing import Any

from mysql.connector.pooling import PooledMySQLConnection  # type: ignore[import-untyped]

from ..db import org_params


class OrgScopedRepository:
    """Base class for repositories operating on tenant-scoped tables."""

    # Subclasses set this for documentation + the static test.
    table_name: str = ""

    def __init__(self, conn: PooledMySQLConnection) -> None:
        if not self.table_name:
            raise TypeError(
                f"{type(self).__name__} must set class attribute `table_name`."
            )
        self.conn = conn

    def params(self, **extra: Any) -> dict[str, Any]:
        """Build a parameter dict that always includes `org_id`."""
        return org_params(**extra)
