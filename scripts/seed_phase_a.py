"""Phase A seed script — idempotent.

Run once on the live DB after `alembic upgrade head` reaches revision 0005:

    cd /home/srpc/srpc_api
    python -m scripts.seed_phase_a

Creates (only if missing):
    * The 6 primary account groups: Capital Account, Loans (Liability),
      Current Liabilities, Fixed Assets, Investments, Current Assets,
      plus Revenue and Expenses top-level nodes (Indian standard COA
      uses Trading & P&L below these — we flatten to match Busy 21).
    * All reserved sub-groups called out in CloudAccountingDesign §4.1:
      Sundry Debtors, Sundry Creditors, Bank Accounts, Cash-in-Hand,
      Duties & Taxes, Stock-in-Hand, Sales Accounts, Purchase Accounts,
      Direct Expenses, Indirect Expenses, Direct Incomes, Indirect Incomes.
    * Financial Year 2526 (1-Apr-2025 to 31-Mar-2026).
    * One 'Default' voucher_series per voucher_type for FY 2526, each
      with a sensible prefix (PV/RV/JV/CV/SV/PU/CN/DN/SJ) and padding=4.
    * Starter ledgers so you can post your first voucher immediately:
      'Cash-in-Hand' under Cash-in-Hand, 'HDFC Bank - Current' under
      Bank Accounts, 'Sales A/c' under Sales Accounts, 'Purchase A/c'
      under Purchase Accounts.

Idempotent: re-running the script is safe. It checks for existence
before every INSERT and logs what it skipped.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date

# Make the repo root importable when run as `python -m scripts.seed_phase_a`
# from /home/srpc/srpc_api.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db import tx  # noqa: E402
from core.tenancy import bind_org  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
)
log = logging.getLogger("seed_phase_a")

SRPC_ORG_ID = 1
FY_CODE = "2526"
FY_START = date(2025, 4, 1)
FY_END = date(2026, 3, 31)

# ---------------------------------------------------------------------------
# COA structure. Each entry: (name, nature, parent_name_or_None, is_reserved,
# affects_gross_profit). Order matters: parents must come before children.
# ---------------------------------------------------------------------------
COA_NODES: list[tuple[str, str, str | None, int, int]] = [
    # ---- Primary groups (6 standard + 2 top-level revenue/expense) ----
    ("Capital Account",      "EQUITY",     None, 1, 0),
    ("Loans (Liability)",    "LIABILITY",  None, 1, 0),
    ("Current Liabilities",  "LIABILITY",  None, 1, 0),
    ("Fixed Assets",         "ASSET",      None, 1, 0),
    ("Investments",          "ASSET",      None, 1, 0),
    ("Current Assets",       "ASSET",      None, 1, 0),
    ("Revenue",              "INCOME",     None, 1, 0),
    ("Expenses",             "EXPENSE",    None, 1, 0),

    # ---- Reserved sub-groups under Current Liabilities ----
    ("Sundry Creditors",     "LIABILITY",  "Current Liabilities", 1, 0),
    ("Duties & Taxes",       "LIABILITY",  "Current Liabilities", 1, 0),

    # ---- Reserved sub-groups under Current Assets ----
    ("Sundry Debtors",       "ASSET",      "Current Assets", 1, 0),
    ("Bank Accounts",        "ASSET",      "Current Assets", 1, 0),
    ("Cash-in-Hand",         "ASSET",      "Current Assets", 1, 0),
    ("Stock-in-Hand",        "ASSET",      "Current Assets", 1, 0),

    # ---- Reserved sub-groups under Revenue ----
    # Sales Accounts + Direct Incomes affect GP; Indirect Incomes don't.
    ("Sales Accounts",       "INCOME",     "Revenue", 1, 1),
    ("Direct Incomes",       "INCOME",     "Revenue", 1, 1),
    ("Indirect Incomes",     "INCOME",     "Revenue", 1, 0),

    # ---- Reserved sub-groups under Expenses ----
    ("Purchase Accounts",    "EXPENSE",    "Expenses", 1, 1),
    ("Direct Expenses",      "EXPENSE",    "Expenses", 1, 1),
    ("Indirect Expenses",    "EXPENSE",    "Expenses", 1, 0),
]

# Voucher series prefixes. Padding = 4 gives e.g. PV0001.
SERIES_PREFIXES = {
    "PAYMENT":       "PV",
    "RECEIPT":       "RV",
    "JOURNAL":       "JV",
    "CONTRA":        "CV",
    "SALES":         "SV",
    "PURCHASE":      "PU",
    "CREDIT_NOTE":   "CN",
    "DEBIT_NOTE":    "DN",
    "STOCK_JOURNAL": "SJ",
}

# Starter ledgers: (name, group_name, is_bank). Kept minimal; add more
# manually or via future admin UI.
STARTER_LEDGERS: list[tuple[str, str, int]] = [
    ("Cash-in-Hand",         "Cash-in-Hand", 0),
    ("HDFC Bank - Current",  "Bank Accounts", 1),
    ("Sales A/c",            "Sales Accounts", 0),
    ("Purchase A/c",         "Purchase Accounts", 0),
]


def _exists_group(cur, name: str, parent_id: int | None) -> int | None:
    cur.execute(
        "SELECT id FROM account_groups "
        "WHERE organization_id = %(org_id)s "
        "  AND name = %(name)s "
        "  AND ((parent_group_id IS NULL AND %(pid)s IS NULL) "
        "       OR parent_group_id = %(pid)s)",
        {"org_id": SRPC_ORG_ID, "name": name, "pid": parent_id},
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _exists_ledger(cur, name: str) -> int | None:
    cur.execute(
        "SELECT id FROM ledgers "
        "WHERE organization_id = %(org_id)s AND name = %(name)s",
        {"org_id": SRPC_ORG_ID, "name": name},
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def seed_account_groups(conn) -> dict[str, int]:
    """Create COA nodes if missing. Returns name -> id map."""
    cur = conn.cursor()
    try:
        name_to_id: dict[str, int] = {}
        for name, nature, parent_name, reserved, agp in COA_NODES:
            parent_id = (
                name_to_id[parent_name] if parent_name is not None else None
            )
            existing = _exists_group(cur, name, parent_id)
            if existing:
                log.info("  [skip] group exists: %s", name)
                name_to_id[name] = existing
                continue
            cur.execute(
                "INSERT INTO account_groups ("
                "    organization_id, parent_group_id, name, nature, "
                "    affects_gross_profit, is_reserved, is_active"
                ") VALUES ("
                "    %(org_id)s, %(pid)s, %(name)s, %(nature)s, "
                "    %(agp)s, %(res)s, 1"
                ")",
                {
                    "org_id": SRPC_ORG_ID,
                    "pid": parent_id,
                    "name": name,
                    "nature": nature,
                    "agp": agp,
                    "res": reserved,
                },
            )
            name_to_id[name] = int(cur.lastrowid)
            log.info("  [new]  group: %s (%s)", name, nature)
        return name_to_id
    finally:
        cur.close()


def seed_financial_year(conn) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM financial_years "
            "WHERE organization_id = %(org_id)s AND code = %(code)s",
            {"org_id": SRPC_ORG_ID, "code": FY_CODE},
        )
        row = cur.fetchone()
        if row:
            log.info("  [skip] FY exists: %s", FY_CODE)
            return int(row[0])
        cur.execute(
            "INSERT INTO financial_years ("
            "    organization_id, code, start_date, end_date, is_locked"
            ") VALUES ("
            "    %(org_id)s, %(code)s, %(s)s, %(e)s, 0"
            ")",
            {
                "org_id": SRPC_ORG_ID,
                "code": FY_CODE,
                "s": FY_START,
                "e": FY_END,
            },
        )
        fy_id = int(cur.lastrowid)
        log.info(
            "  [new]  FY %s (%s to %s)", FY_CODE, FY_START, FY_END
        )
        return fy_id
    finally:
        cur.close()


def seed_voucher_series(conn, fy_id: int) -> None:
    cur = conn.cursor()
    try:
        for vt, prefix in SERIES_PREFIXES.items():
            cur.execute(
                "SELECT id FROM voucher_series "
                "WHERE organization_id = %(org_id)s "
                "  AND financial_year_id = %(fy)s "
                "  AND voucher_type = %(vt)s "
                "  AND name = 'Default'",
                {"org_id": SRPC_ORG_ID, "fy": fy_id, "vt": vt},
            )
            if cur.fetchone():
                log.info("  [skip] series exists: %s Default", vt)
                continue
            cur.execute(
                "INSERT INTO voucher_series ("
                "    organization_id, financial_year_id, voucher_type, "
                "    name, prefix, suffix, padding, next_number, is_active"
                ") VALUES ("
                "    %(org_id)s, %(fy)s, %(vt)s, 'Default', "
                "    %(pre)s, '', 4, 1, 1"
                ")",
                {
                    "org_id": SRPC_ORG_ID,
                    "fy": fy_id,
                    "vt": vt,
                    "pre": prefix,
                },
            )
            log.info("  [new]  series: %s Default (%s0001 onwards)", vt, prefix)
    finally:
        cur.close()


def seed_starter_ledgers(conn, group_ids: dict[str, int]) -> None:
    cur = conn.cursor()
    try:
        for name, group_name, is_bank in STARTER_LEDGERS:
            if _exists_ledger(cur, name):
                log.info("  [skip] ledger exists: %s", name)
                continue
            cur.execute(
                "INSERT INTO ledgers ("
                "    organization_id, group_id, name, is_bank, "
                "    is_reserved, is_active, opening_balance, "
                "    opening_balance_date"
                ") VALUES ("
                "    %(org_id)s, %(gid)s, %(name)s, %(is_bank)s, "
                "    1, 1, 0.00, %(obd)s"
                ")",
                {
                    "org_id": SRPC_ORG_ID,
                    "gid": group_ids[group_name],
                    "name": name,
                    "is_bank": is_bank,
                    "obd": FY_START,
                },
            )
            log.info("  [new]  ledger: %s (under %s)", name, group_name)
    finally:
        cur.close()


def main() -> None:
    log.info("Seeding Phase A for organization_id=%d", SRPC_ORG_ID)
    with bind_org(SRPC_ORG_ID), tx() as conn:
        log.info("Step 1: account_groups")
        group_ids = seed_account_groups(conn)

        log.info("Step 2: financial_years")
        fy_id = seed_financial_year(conn)

        log.info("Step 3: voucher_series")
        seed_voucher_series(conn, fy_id)

        log.info("Step 4: starter ledgers")
        seed_starter_ledgers(conn, group_ids)

    log.info("Done.")


if __name__ == "__main__":
    main()
