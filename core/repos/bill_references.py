"""Bill references repository.

`outstanding_amount` is maintained explicitly by the settlement service.
This repo provides the atomic recompute helper — always call it inside
the same transaction as the allocation insert/reverse.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from .base import OrgScopedRepository


class BillReferenceRepository(OrgScopedRepository):
    table_name = "bill_references"

    def get_by_id(self, bill_id: int, *, for_update: bool = False) -> Optional[dict]:
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, organization_id, party_ledger_id, bill_no, "
                "       bill_date, due_date, original_amount, "
                "       outstanding_amount, side, status, source_voucher_id "
                "FROM bill_references "
                "WHERE organization_id = %(org_id)s AND id = %(bid)s "
                + ("FOR UPDATE" if for_update else ""),
                self.params(bid=bill_id),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def insert(
        self,
        *,
        party_ledger_id: int,
        bill_no: str,
        bill_date: date,
        original_amount: Decimal,
        side: str,
        source_voucher_id: Optional[int] = None,
        due_date: Optional[date] = None,
        notes: Optional[str] = None,
    ) -> int:
        if side not in ("RECEIVABLE", "PAYABLE"):
            raise ValueError(f"side must be RECEIVABLE or PAYABLE, got {side!r}")
        cur = self.conn.cursor()
        try:
            cur.execute(
                "INSERT INTO bill_references ("
                "    organization_id, party_ledger_id, bill_no, bill_date, "
                "    due_date, original_amount, outstanding_amount, "
                "    source_voucher_id, side, status, notes"
                ") VALUES ("
                "    %(org_id)s, %(pl)s, %(bn)s, %(bd)s, %(dd)s, "
                "    %(amt)s, %(amt)s, %(svid)s, %(side)s, 'OPEN', %(notes)s"
                ")",
                self.params(
                    pl=party_ledger_id,
                    bn=bill_no,
                    bd=bill_date,
                    dd=due_date,
                    amt=original_amount,
                    svid=source_voucher_id,
                    side=side,
                    notes=notes,
                ),
            )
            return int(cur.lastrowid)
        finally:
            cur.close()

    def recompute_outstanding(self, bill_id: int) -> None:
        """Recompute outstanding_amount and status for a single bill.

        Called inside the same transaction as every allocation insert/
        reverse. Safe under concurrency because the caller is expected
        to hold a SELECT ... FOR UPDATE on the bill row.
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                "UPDATE bill_references br "
                "SET outstanding_amount = br.original_amount - COALESCE(("
                "       SELECT SUM(a.amount) FROM allocations a "
                "       WHERE a.organization_id = br.organization_id "
                "         AND a.bill_reference_id = br.id "
                "         AND a.is_reversed = 0"
                "    ), 0), "
                "    status = CASE "
                "        WHEN br.original_amount - COALESCE(("
                "           SELECT SUM(a.amount) FROM allocations a "
                "           WHERE a.organization_id = br.organization_id "
                "             AND a.bill_reference_id = br.id "
                "             AND a.is_reversed = 0"
                "        ), 0) <= 0 THEN 'CLEARED' "
                "        WHEN br.original_amount - COALESCE(("
                "           SELECT SUM(a.amount) FROM allocations a "
                "           WHERE a.organization_id = br.organization_id "
                "             AND a.bill_reference_id = br.id "
                "             AND a.is_reversed = 0"
                "        ), 0) = br.original_amount THEN 'OPEN' "
                "        ELSE 'PARTIAL' "
                "    END "
                "WHERE br.organization_id = %(org_id)s AND br.id = %(bid)s",
                self.params(bid=bill_id),
            )
        finally:
            cur.close()

    def list_outstanding(
        self,
        *,
        party_ledger_id: Optional[int] = None,
        side: Optional[str] = None,
    ) -> list[dict]:
        """Open / partial bills, optionally filtered by party and side."""
        clauses = ["organization_id = %(org_id)s",
                   "status IN ('OPEN', 'PARTIAL')"]
        params: dict = self.params()
        if party_ledger_id is not None:
            clauses.append("party_ledger_id = %(pl)s")
            params["pl"] = party_ledger_id
        if side is not None:
            if side not in ("RECEIVABLE", "PAYABLE"):
                raise ValueError("side must be RECEIVABLE or PAYABLE")
            clauses.append("side = %(side)s")
            params["side"] = side

        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, party_ledger_id, bill_no, bill_date, due_date, "
                "       original_amount, outstanding_amount, side, status, "
                "       source_voucher_id "
                "FROM bill_references "
                "WHERE " + " AND ".join(clauses) + " "
                "ORDER BY bill_date, id",
                params,
            )
            return list(cur.fetchall())
        finally:
            cur.close()
