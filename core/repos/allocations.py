"""Allocations repository — append-only, reversible-in-place via flag."""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from .base import OrgScopedRepository


class AllocationRepository(OrgScopedRepository):
    table_name = "allocations"

    def insert(
        self,
        *,
        allocating_voucher_id: int,
        bill_reference_id: int,
        amount: Decimal,
    ) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(
                "INSERT INTO allocations ("
                "    organization_id, allocating_voucher_id, "
                "    bill_reference_id, amount, is_reversed"
                ") VALUES ("
                "    %(org_id)s, %(vid)s, %(bid)s, %(amt)s, 0"
                ")",
                self.params(
                    vid=allocating_voucher_id,
                    bid=bill_reference_id,
                    amt=amount,
                ),
            )
            return int(cur.lastrowid)
        finally:
            cur.close()

    def mark_reversed_for_voucher(self, voucher_id: int) -> list[int]:
        """Flip is_reversed=1 on every allocation of a voucher.

        Returns the list of affected bill_reference_ids so the caller
        (voucher cancel service) can call BillReferenceRepository
        .recompute_outstanding() for each.
        """
        # First, discover which bills are affected (for the recompute list).
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT DISTINCT bill_reference_id FROM allocations "
                "WHERE organization_id = %(org_id)s "
                "  AND allocating_voucher_id = %(vid)s "
                "  AND is_reversed = 0",
                self.params(vid=voucher_id),
            )
            bill_ids = [int(row["bill_reference_id"]) for row in cur.fetchall()]
        finally:
            cur.close()

        if not bill_ids:
            return []

        cur = self.conn.cursor()
        try:
            cur.execute(
                "UPDATE allocations "
                "SET is_reversed = 1, reversed_at = NOW() "
                "WHERE organization_id = %(org_id)s "
                "  AND allocating_voucher_id = %(vid)s "
                "  AND is_reversed = 0",
                self.params(vid=voucher_id),
            )
        finally:
            cur.close()
        return bill_ids

    def sum_active_for_voucher(self, voucher_id: int) -> Decimal:
        """Sum of non-reversed allocations for a voucher."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM allocations "
                "WHERE organization_id = %(org_id)s "
                "  AND allocating_voucher_id = %(vid)s "
                "  AND is_reversed = 0",
                self.params(vid=voucher_id),
            )
            row = cur.fetchone()
            return Decimal(row[0]) if row and row[0] is not None else Decimal("0")
        finally:
            cur.close()

    def list_for_voucher(self, voucher_id: int) -> list[dict]:
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT a.id, a.allocating_voucher_id, a.bill_reference_id, "
                "       a.amount, a.is_reversed, a.allocated_at, "
                "       b.bill_no "
                "FROM allocations a "
                "JOIN bill_references b "
                "  ON b.id = a.bill_reference_id "
                " AND b.organization_id = a.organization_id "
                "WHERE a.organization_id = %(org_id)s "
                "  AND a.allocating_voucher_id = %(vid)s "
                "ORDER BY a.id",
                self.params(vid=voucher_id),
            )
            return list(cur.fetchall())
        finally:
            cur.close()
