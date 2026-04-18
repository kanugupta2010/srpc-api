"""Shared pytest fixtures for accounting-core tests.

Design note: tests use a dedicated `organization_id` (9999) created and
torn down per test session, rather than savepoints, because mysql.connector's
pooled connections don't play nicely with nested SAVEPOINT reuse across
our tx() helper. Each test creates the rows it needs and leaves them;
teardown deletes everything for org 9999 in FK-safe order.

The test DB is the SAME MySQL as production. Tests are expected to run
against a dev/staging instance, never against live. The test org isolates
data so an accidental run against a wrong DB doesn't corrupt real data.
"""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal

import pytest

# Ensure tests use env vars; fail fast if they aren't set.
for var in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"):
    if not os.environ.get(var):  # pragma: no cover
        pytest.skip(
            f"{var} not set — accounting tests require a dev MySQL",
            allow_module_level=True,
        )

from core.db import tx  # noqa: E402
from core.tenancy import bind_org  # noqa: E402

TEST_ORG_ID = 9999
TEST_FY_CODE = "TEST"

# Tables we clean up per session, in FK-safe order.
_TENANT_TABLES_TO_CLEAN = [
    # Phase C — must precede vouchers (allocations FK to vouchers,
    # bill_references FK to vouchers).
    "audit_log",
    "allocations",
    "bill_references",
    # Phase A + B
    "voucher_lines",
    "vouchers",
    "voucher_series",
    "financial_years",
    "parties",
    "ledgers",
    "account_groups",
]


@pytest.fixture(scope="session", autouse=True)
def _test_org():
    """Create (and finally delete) a throwaway organization for tests."""
    with tx() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO organizations "
                "  (id, code, legal_name, display_name, state_code) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE code = VALUES(code)",
                (TEST_ORG_ID, "TEST", "Test Org", "Test Org", "07"),
            )
        finally:
            cur.close()

    yield TEST_ORG_ID

    # Teardown — delete every tenant-scoped row we may have created.
    with tx() as conn:
        cur = conn.cursor()
        try:
            for table in _TENANT_TABLES_TO_CLEAN:
                cur.execute(
                    f"DELETE FROM {table} WHERE organization_id = %s",
                    (TEST_ORG_ID,),
                )
            cur.execute(
                "DELETE FROM organizations WHERE id = %s", (TEST_ORG_ID,)
            )
        finally:
            cur.close()


@pytest.fixture
def coa(_test_org):
    """Seed a minimal COA + FY + series + ledgers for every test.

    Cleans up at the end of each test so tests don't interfere with
    each other on voucher_series.next_number.
    """
    created_ids: dict[str, int] = {}

    with bind_org(TEST_ORG_ID), tx() as conn:
        cur = conn.cursor()
        try:
            # Minimal group tree: Assets root + Cash-in-Hand + Bank Accounts
            # + Sales Accounts + Purchase Accounts + Sundry Debtors.
            cur.execute(
                "INSERT INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, NULL, 'Current Assets', 'ASSET', 0, 1)",
                (TEST_ORG_ID,),
            )
            ca = int(cur.lastrowid)

            cur.execute(
                "INSERT INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, %s, 'Cash-in-Hand', 'ASSET', 0, 1)",
                (TEST_ORG_ID, ca),
            )
            cash_group = int(cur.lastrowid)

            cur.execute(
                "INSERT INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, %s, 'Bank Accounts', 'ASSET', 0, 1)",
                (TEST_ORG_ID, ca),
            )
            bank_group = int(cur.lastrowid)

            cur.execute(
                "INSERT INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, %s, 'Sundry Debtors', 'ASSET', 0, 1)",
                (TEST_ORG_ID, ca),
            )
            debtors_group = int(cur.lastrowid)

            cur.execute(
                "INSERT INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, NULL, 'Revenue', 'INCOME', 0, 1)",
                (TEST_ORG_ID,),
            )
            rev = int(cur.lastrowid)

            cur.execute(
                "INSERT INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, %s, 'Sales Accounts', 'INCOME', 1, 1)",
                (TEST_ORG_ID, rev),
            )
            sales_group = int(cur.lastrowid)

            # Ledgers
            for nm, gid, is_bank in [
                ("Cash-in-Hand", cash_group, 0),
                ("HDFC Bank", bank_group, 1),
                ("ICICI Bank", bank_group, 1),
                ("Sales A/c", sales_group, 0),
                ("ABC Traders", debtors_group, 0),
            ]:
                cur.execute(
                    "INSERT INTO ledgers "
                    "(organization_id, group_id, name, is_bank, is_active) "
                    "VALUES (%s, %s, %s, %s, 1)",
                    (TEST_ORG_ID, gid, nm, is_bank),
                )
                created_ids[nm] = int(cur.lastrowid)

            # FY
            cur.execute(
                "INSERT INTO financial_years "
                "(organization_id, code, start_date, end_date, is_locked) "
                "VALUES (%s, %s, %s, %s, 0)",
                (TEST_ORG_ID, TEST_FY_CODE, date(2025, 4, 1), date(2026, 3, 31)),
            )
            fy_id = int(cur.lastrowid)
            created_ids["__fy_id__"] = fy_id

            # Default series for every type
            for vt, pre in [
                ("PAYMENT", "PV"), ("RECEIPT", "RV"), ("JOURNAL", "JV"),
                ("CONTRA", "CV"), ("SALES", "SV"), ("PURCHASE", "PU"),
                ("CREDIT_NOTE", "CN"), ("DEBIT_NOTE", "DN"),
                ("STOCK_JOURNAL", "SJ"),
            ]:
                cur.execute(
                    "INSERT INTO voucher_series "
                    "(organization_id, financial_year_id, voucher_type, "
                    " name, prefix, suffix, padding, next_number, is_active) "
                    "VALUES (%s, %s, %s, 'Default', %s, '', 4, 1, 1)",
                    (TEST_ORG_ID, fy_id, vt, pre),
                )
        finally:
            cur.close()

    yield created_ids

    # Per-test teardown — remove everything we inserted so voucher
    # numbers restart at 1 next test.
    with tx() as conn:
        cur = conn.cursor()
        try:
            for table in _TENANT_TABLES_TO_CLEAN:
                cur.execute(
                    f"DELETE FROM {table} WHERE organization_id = %s",
                    (TEST_ORG_ID,),
                )
        finally:
            cur.close()


@pytest.fixture
def bound():
    """Bind the test organization_id for the duration of one test."""
    with bind_org(TEST_ORG_ID):
        yield TEST_ORG_ID


def D(v) -> Decimal:  # noqa: N802 — tiny shorthand for tests
    return Decimal(str(v))
