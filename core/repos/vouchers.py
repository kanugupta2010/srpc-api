"""Voucher repository — header + line persistence."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from .base import OrgScopedRepository


class VoucherRepository(OrgScopedRepository):
    table_name = "vouchers"

    def insert_header(
        self,
        *,
        voucher_type: str,
        voucher_series_id: int,
        financial_year_id: int,
        voucher_number: str,
        voucher_date: date,
        reference_no: Optional[str],
        party_ledger_id: Optional[int],
        narration: Optional[str],
        total_amount: Decimal,
        source_doc_type: Optional[str],
        source_doc_id: Optional[int],
        created_by: Optional[str],
    ) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(
                "INSERT INTO vouchers ("
                "    organization_id, voucher_type, voucher_series_id, "
                "    financial_year_id, voucher_number, voucher_date, "
                "    reference_no, party_ledger_id, narration, total_amount, "
                "    status, posted_at, source_doc_type, source_doc_id, "
                "    created_by"
                ") VALUES ("
                "    %(org_id)s, %(vt)s, %(vsid)s, %(fyid)s, %(vn)s, %(vd)s, "
                "    %(ref)s, %(pl)s, %(nar)s, %(amt)s, "
                "    'POSTED', NOW(), %(sdt)s, %(sdi)s, %(cb)s"
                ")",
                self.params(
                    vt=voucher_type,
                    vsid=voucher_series_id,
                    fyid=financial_year_id,
                    vn=voucher_number,
                    vd=voucher_date,
                    ref=reference_no,
                    pl=party_ledger_id,
                    nar=narration,
                    amt=total_amount,
                    sdt=source_doc_type,
                    sdi=source_doc_id,
                    cb=created_by,
                ),
            )
            return int(cur.lastrowid)
        finally:
            cur.close()

    def insert_line(
        self,
        *,
        voucher_id: int,
        ledger_id: int,
        dr_cr: str,
        amount: Decimal,
        cost_center_id: Optional[int],
        line_narration: Optional[str],
        line_order: int,
    ) -> int:
        if dr_cr not in ("Dr", "Cr"):
            raise ValueError(f"dr_cr must be 'Dr' or 'Cr', got {dr_cr!r}")
        cur = self.conn.cursor()
        try:
            cur.execute(
                "INSERT INTO voucher_lines ("
                "    organization_id, voucher_id, ledger_id, dr_cr, amount, "
                "    cost_center_id, line_narration, line_order"
                ") VALUES ("
                "    %(org_id)s, %(vid)s, %(lid)s, %(dc)s, %(amt)s, "
                "    %(cc)s, %(ln)s, %(lo)s"
                ")",
                self.params(
                    vid=voucher_id,
                    lid=ledger_id,
                    dc=dr_cr,
                    amt=amount,
                    cc=cost_center_id,
                    ln=line_narration,
                    lo=line_order,
                ),
            )
            return int(cur.lastrowid)
        finally:
            cur.close()

    def get_header(self, voucher_id: int) -> Optional[dict]:
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, organization_id, voucher_type, voucher_series_id, "
                "       financial_year_id, voucher_number, voucher_date, "
                "       reference_no, party_ledger_id, narration, "
                "       total_amount, status, posted_at, cancelled_at, "
                "       source_doc_type, source_doc_id, created_by, "
                "       created_at, updated_at "
                "FROM vouchers "
                "WHERE organization_id = %(org_id)s AND id = %(vid)s",
                self.params(vid=voucher_id),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def get_lines(self, voucher_id: int) -> list[dict]:
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, voucher_id, ledger_id, dr_cr, amount, "
                "       cost_center_id, line_narration, line_order "
                "FROM voucher_lines "
                "WHERE organization_id = %(org_id)s AND voucher_id = %(vid)s "
                "ORDER BY line_order, id",
                self.params(vid=voucher_id),
            )
            return list(cur.fetchall())
        finally:
            cur.close()

    def mark_cancelled(self, voucher_id: int) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(
                "UPDATE vouchers "
                "SET status = 'CANCELLED', cancelled_at = NOW() "
                "WHERE organization_id = %(org_id)s "
                "  AND id = %(vid)s "
                "  AND status = 'POSTED'",
                self.params(vid=voucher_id),
            )
            if cur.rowcount != 1:
                # Not posted, already cancelled, or wrong org.
                from ..errors import (
                    VoucherAlreadyCancelledError,
                    VoucherNotPostedError,
                )

                # Disambiguate with a follow-up read.
                header = self.get_header(voucher_id)
                if header is None:
                    raise VoucherNotPostedError(
                        "Voucher does not exist or is not in active org.",
                        voucher_id=voucher_id,
                    )
                if header["status"] == "CANCELLED":
                    raise VoucherAlreadyCancelledError(
                        "Voucher is already cancelled.",
                        voucher_id=voucher_id,
                    )
                raise VoucherNotPostedError(
                    f"Voucher status is {header['status']}, expected POSTED.",
                    voucher_id=voucher_id,
                )
        finally:
            cur.close()
