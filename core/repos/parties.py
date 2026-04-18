"""Parties repository — CRUD + auto-ledger creation.

The party<->ledger pairing is set up by the seed script and by the
party-creation service (to be added in Phase C alongside the first
admin endpoint that creates a party). This module currently exposes
the lookup operations used during voucher posting validation.
"""
from __future__ import annotations

from typing import Optional

from .base import OrgScopedRepository


class PartyRepository(OrgScopedRepository):
    table_name = "parties"

    def get_by_id(self, party_id: int) -> Optional[dict]:
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, organization_id, party_type, name, mobile, "
                "       gstin, ledger_id, is_active "
                "FROM parties "
                "WHERE organization_id = %(org_id)s AND id = %(pid)s",
                self.params(pid=party_id),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def get_ledger_id_for_party(self, party_id: int) -> Optional[int]:
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT ledger_id FROM parties "
                "WHERE organization_id = %(org_id)s AND id = %(pid)s",
                self.params(pid=party_id),
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
        finally:
            cur.close()
