"""Ledgers repository — lookups used by the posting service."""
from __future__ import annotations

from typing import Optional

from .base import OrgScopedRepository


class LedgerRepository(OrgScopedRepository):
    table_name = "ledgers"

    def get_by_id(self, ledger_id: int) -> Optional[dict]:
        """Fetch a ledger by id, scoped to the active org."""
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, organization_id, group_id, name, is_party, "
                "       party_id, is_active "
                "FROM ledgers "
                "WHERE organization_id = %(org_id)s AND id = %(ledger_id)s",
                self.params(ledger_id=ledger_id),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def get_by_name(self, name: str) -> Optional[dict]:
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, organization_id, group_id, name, is_party, "
                "       party_id, is_active "
                "FROM ledgers "
                "WHERE organization_id = %(org_id)s AND name = %(name)s",
                self.params(name=name),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def assert_all_belong_to_org(self, ledger_ids: list[int]) -> None:
        """Raise if any of the ledger_ids does not belong to the active org."""
        if not ledger_ids:
            return
        # Build a placeholder for the IN clause; mysql.connector named
        # parameters don't expand sequences, so we build explicit names.
        names = [f"id_{i}" for i in range(len(ledger_ids))]
        placeholders = ", ".join(f"%({n})s" for n in names)
        params = self.params(**dict(zip(names, ledger_ids)))
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"SELECT id FROM ledgers "
                f"WHERE organization_id = %(org_id)s "
                f"  AND id IN ({placeholders})",
                params,
            )
            found = {row["id"] for row in cur.fetchall()}
            missing = set(ledger_ids) - found
            if missing:
                from ..errors import CrossOrgReferenceError

                raise CrossOrgReferenceError(
                    "One or more ledgers do not belong to the active "
                    "organization or do not exist.",
                    missing_ledger_ids=sorted(missing),
                )
        finally:
            cur.close()

    def assert_all_active(self, ledger_ids: list[int]) -> None:
        if not ledger_ids:
            return
        names = [f"id_{i}" for i in range(len(ledger_ids))]
        placeholders = ", ".join(f"%({n})s" for n in names)
        params = self.params(**dict(zip(names, ledger_ids)))
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"SELECT id FROM ledgers "
                f"WHERE organization_id = %(org_id)s "
                f"  AND id IN ({placeholders}) "
                f"  AND is_active = 0",
                params,
            )
            inactive = [row["id"] for row in cur.fetchall()]
            if inactive:
                from ..errors import LedgerInactiveError

                raise LedgerInactiveError(
                    "One or more ledgers are inactive.",
                    inactive_ledger_ids=inactive,
                )
        finally:
            cur.close()
