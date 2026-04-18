# Accounting Core — Phase A + B

This directory is the greenfield accounting core per
`CloudAccountingDesign.docx`. It runs **alongside** the existing
loyalty program code; nothing under `routers/`, `services/` (outside
`core/services/`), `models/`, or `recalculate_points.py` is touched.

## First-time deployment (live DB)

The live MySQL already has the loyalty schema. We do NOT want Alembic
to re-run those CREATE TABLEs. Stamp the live DB at the baseline:

```bash
cd /home/srpc/srpc_api
pip install -r requirements.txt   # pulls alembic, sqlalchemy, sqlparse, etc.

# Environment for Alembic — same vars the app uses
export $(grep -v '^#' .env | xargs)

# Tell Alembic the existing schema is at 0001 (no-op migration).
alembic stamp 0001

# Apply Phase A + B migrations.
alembic upgrade head
```

After `upgrade head` the DB has:
  - `organizations`, `account_groups`, `ledgers`, `parties`
  - `financial_years`, `voucher_series`
  - `vouchers`, `voucher_lines`
  - `organization_id` backfilled to `1` on every existing tenant-scoped
    table (item_master, contractors, invoices, ... all twelve).

Seed the SRPC chart of accounts, FY 2526, voucher series, and a
starter set of ledgers:

```bash
python -m scripts.seed_phase_a
```

The seed script is idempotent — safe to re-run.

## Fresh environment (new dev box or a second organization)

```bash
# 1. Apply the legacy schema first (creates loyalty tables).
mysql -h $DB_HOST -u $DB_USER -p$DB_PASSWORD $DB_NAME \
    < srpc_loyalty_schema.sql

# 2. Stamp at 0001, then upgrade.
alembic stamp 0001
alembic upgrade head

# 3. Seed.
python -m scripts.seed_phase_a
```

## Running tests

Tests require a dev MySQL (not production). They create and destroy
`organization_id = 9999`.

```bash
export DB_HOST=... DB_NAME=... DB_USER=... DB_PASSWORD=...
pytest tests/ -v
```

The static test (`test_static_org_scope.py`) is the one to watch: it
fails the suite if any SQL in `core/` or `scripts/` touches a
tenant-scoped table without filtering by `organization_id`. When you
add a new tenant-scoped table in a future migration, add its name to
`TENANT_TABLES` in that test.

## Package layout

```
core/
├── errors.py              domain exceptions with stable error codes
├── tenancy.py             active_org_id contextvar + FastAPI dep
├── db.py                  tx() transaction context manager
├── repos/                 per-entity SQL, no business logic
│   ├── base.py            OrgScopedRepository convention base
│   ├── ledgers.py
│   ├── parties.py
│   ├── voucher_series.py  includes SELECT ... FOR UPDATE issuance
│   └── vouchers.py
└── services/
    └── posting_service.py the single chokepoint for creating vouchers
```

## How to post a voucher from application code

Inside a FastAPI endpoint (Phase C will wire these up):

```python
from datetime import date
from decimal import Decimal

from core.tenancy import bind_org
from core.services.posting_service import (
    VoucherInput, VoucherLineInput, post_voucher,
)

with bind_org(active_org_id):   # from the JWT dependency in Phase C
    posted = post_voucher(
        VoucherInput(
            voucher_type="JOURNAL",
            voucher_date=date(2025, 5, 10),
            narration="Opening cash balance",
            lines=[
                VoucherLineInput(cash_ledger_id,    "Dr", Decimal("50000")),
                VoucherLineInput(capital_ledger_id, "Cr", Decimal("50000")),
            ],
        )
    )
# posted.voucher_number == 'JV0001'
```

`post_voucher()` either succeeds completely or raises a `DomainError`
subclass; nothing is ever half-written.

## What Phase A + B does NOT include (coming next)

- Purchase / Sales invoice models and endpoints (Phase E)
- Inventory (items, stock_movements, valuation) (Phase D)
- Settlement / bill-wise allocation (Phase C)
- Payment / Receipt / Journal / Contra endpoints themselves (Phase C)
- Any integration with the existing Busy import path (Phase F)

The posting engine exists and can post vouchers right now — but nothing
in the admin UI calls it yet. Phase C builds the first endpoints.
