"""Voucher series + financial year lookups, with locked next-number issuance."""
from __future__ import annotations

from datetime import date
from typing import Optional

from ..errors import (
    FinancialYearNotFoundError,
    VoucherPeriodLockedError,
    VoucherSeriesNotFoundError,
)
from .base import OrgScopedRepository


class FinancialYearRepository(OrgScopedRepository):
    table_name = "financial_years"

    def get_for_date(self, on_date: date) -> dict:
        """Return the (unlocked) financial year covering the given date."""
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, code, start_date, end_date, is_locked "
                "FROM financial_years "
                "WHERE organization_id = %(org_id)s "
                "  AND %(d)s BETWEEN start_date AND end_date",
                self.params(d=on_date),
            )
            row = cur.fetchone()
            if not row:
                raise FinancialYearNotFoundError(
                    f"No financial_year covers {on_date.isoformat()} for "
                    "the active organization.",
                    on_date=on_date.isoformat(),
                )
            if row["is_locked"]:
                raise VoucherPeriodLockedError(
                    f"Financial year {row['code']} is locked.",
                    fy_code=row["code"],
                    on_date=on_date.isoformat(),
                )
            return row
        finally:
            cur.close()


class VoucherSeriesRepository(OrgScopedRepository):
    table_name = "voucher_series"

    def get_default_for(
        self, voucher_type: str, financial_year_id: int
    ) -> Optional[dict]:
        """Return the 'Default' series for a (type, FY) combo, if any."""
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, voucher_type, name, prefix, suffix, padding, "
                "       next_number "
                "FROM voucher_series "
                "WHERE organization_id = %(org_id)s "
                "  AND voucher_type = %(vt)s "
                "  AND financial_year_id = %(fy)s "
                "  AND name = 'Default' "
                "  AND is_active = 1",
                self.params(vt=voucher_type, fy=financial_year_id),
            )
            return cur.fetchone()
        finally:
            cur.close()

    def issue_next_number(self, series_id: int) -> str:
        """Atomically lock the series row, increment, return formatted number.

        MUST be called inside an open transaction. The SELECT ... FOR
        UPDATE serializes concurrent posts; the UNIQUE constraint on
        vouchers (org, type, fy, voucher_number) is the backstop.
        """
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, prefix, suffix, padding, next_number "
                "FROM voucher_series "
                "WHERE organization_id = %(org_id)s AND id = %(sid)s "
                "FOR UPDATE",
                self.params(sid=series_id),
            )
            row = cur.fetchone()
            if not row:
                raise VoucherSeriesNotFoundError(
                    "Voucher series not found or not in active organization.",
                    series_id=series_id,
                )
            number_body = str(row["next_number"]).zfill(int(row["padding"]))
            voucher_number = f"{row['prefix']}{number_body}{row['suffix']}"

            cur.execute(
                "UPDATE voucher_series "
                "SET next_number = next_number + 1 "
                "WHERE organization_id = %(org_id)s AND id = %(sid)s",
                self.params(sid=series_id),
            )
            return voucher_number
        finally:
            cur.close()
