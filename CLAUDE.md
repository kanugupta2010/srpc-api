# SRPC API — Claude Context File

Read this file at the start of every session before touching any code.

---

## Infrastructure

| Component | Details |
|---|---|
| API | FastAPI · DigitalOcean droplet `srpcconnect` · `https://api.saraswatiretail.com` · port 8000 · systemd `srpc_api` |
| DB | AWS RDS MySQL · `crons-dcrm-rds.chuoeciigjit.ap-south-1.rds.amazonaws.com:3306` · database `contractorconnect` |
| Admin Dashboard | React/Vite · `admin.saraswatiretail.com` · Vercel · repo `srpc-admin-dashboard` |
| Contractor App | React/Vite · `rewards.saraswatiretail.com` · Vercel |
| Company Code | `SRPC` (DEFAULT_COMPANY constant used everywhere) |

## Deploy Commands

```bash
# API
git push
ssh srpcconnect "cd /home/srpc/srpc_api && git pull && sudo systemctl restart srpc_api"

# Admin dashboard (separate repo)
cd srpc-admin-dashboard && vercel --prod
```

---

## Project Structure

```
srpc-api/
├── main.py                          # FastAPI app, CORS, router registration
├── recalculate_points.py            # Standalone script — run directly on server
├── routers/
│   ├── admin.py                     # Sales import, contractor listing
│   ├── auth.py                      # JWT login
│   ├── inventory.py                 # Stock, ledger, tags, thresholds
│   ├── reports.py                   # All report endpoints (voucher/item wise)
│   └── sync.py                      # Item master sync, recalculate points endpoint
├── services/
│   ├── import_service.py            # Sales XLSX/CSV parser
│   ├── points_engine.py             # Points calculation during import
│   ├── purchase_import_service.py   # Purchase XLSX/CSV parser
│   └── purchase_service.py         # Purchase invoice processor
├── models/
│   └── schemas.py                   # Pydantic models
└── services/
    └── dependencies.py              # require_admin, get_connection
```

---

## Database Schema (key tables)

| Table | Key Columns |
|---|---|
| `companies` | `company_code`, `gst_applicable` (SRPC=0) |
| `item_master` | `item_code`, `actual_quantity` (e.g. `18lt`), `tags_raw`, `bill_landing`, `reorder_threshold`, `earns_points`, `points_rate`, `is_active` |
| `item_tags` | `id`, `company_code`, `tag_name` — UNIQUE(company_code, tag_name) |
| `item_tag_map` | `company_code`, `item_code`, `tag_id` — FK to item_tags ON DELETE CASCADE |
| `invoices` | `bill_number`, `invoice_type` (sale/sale_return), `financial_year` (e.g. 2526), `contractor_id`, `points_awarded`, `points_status` |
| `invoice_lines` | `invoice_id`, `item_code`, `quantity`, `unit_price`, `line_amount` |
| `purchase_invoices` | `bill_number`, `invoice_type` (purchase/purchase_return), `supplier_name`, `gross_amount_inc`, `gross_amount_exc` |
| `purchase_lines` | `purchase_invoice_id`, `item_code`, `quantity`, `unit_price_inc`, `line_amount_inc`, `line_amount_exc` |
| `contractors` | `id`, `contractor_code`, `status`, `points_balance`, `total_points_earned`, `tier` |
| `points_log` | `contractor_id`, `invoice_id`, `event_type` (earned/reversed/redeemed/expired/adjusted), `points` |
| `settings` | `key_name`, `key_value` — includes `points_base`, `points_expiry_days`, `tier_gold_min`, `tier_platinum_min` |
| `vw_stock_summary` | VIEW — includes `current_stock`, `bill_landing`, `reorder_threshold`, `needs_reorder` |

---

## Critical Business Rules

### Points
- **Formula:** `math.floor(abs(amount) * points_rate)` — `points_rate` already encodes the base
  - `points_rate = 0.01` means 1 point per ₹100 → `floor(9646 * 0.01) = 96`
  - Do NOT divide by `points_base` — that double-counts the base
- **Storage:** Always whole integers — never `round(..., 2)` on points values
- **`recalculate_points.py` and `points_engine.py` must use identical formula**

### GST
- SRPC has `gst_applicable = 0` → prices stored as-is, no GST calculation
- Purchase prices stored inclusive of GST as entered

### Financial Year
- Format: `YYNN` e.g. `2526` = FY 2025-26
- UNIQUE constraint on `(company_code, bill_number, invoice_type, financial_year)`

### Returns
- Sale returns must be **subtracted** (negative) in item-wise report totals
- SQL: `CASE WHEN invoice_type='sale_return' THEN -ABS(amount) ELSE ABS(amount) END`
- Same for purchase returns

### Stock / Reorder
- `needs_reorder = current_stock < reorder_threshold` (strictly less than)
- Running stock: calculate in ASC order (oldest→newest), then reverse for DESC display

---

## File Dependency Map

**Changes to one file often require changes to another:**

| If you change... | Also check... |
|---|---|
| `points_engine.py` — points formula | `recalculate_points.py` — must use identical formula |
| `inventory.py` — `get_item_ledger` | `cursor.close()` must come AFTER opening stock queries |
| `reports.py` — item-wise report SQL | Both sales AND purchases need same treatment (returns as negative) |
| `reports.py` — `_compute_unit_totals` | Only aggregates items WITH `actual_quantity` — items without are skipped |
| `sync.py` — item sync | Tags are synced via `_sync_tags()` — item_tag_map is wiped and reinserted per item |
| Admin dashboard `App.jsx` — `SortableTable` | Used in 4 report pages: sales-vouchers, sales-items, purchases-vouchers, purchases-items |
| Admin dashboard `App.jsx` — `ItemLedger` | Used in StockPage, SalesItemsPage, PurchasesItemsPage, TagsPage |
| Admin dashboard `App.jsx` — `MultiSelectDropdown` | Used via `TagFilterDropdown` wrapper — both must stay in sync |

---

## Known Gotchas (hard-won fixes)

### Backend
1. **`cursor.close()` in `get_item_ledger`** — must happen AFTER the opening stock queries (`if date_from` block). Moving it before causes `ProgrammingError: cursor closed` → 500 → "Failed to fetch" in browser.
2. **`import re` must be at module top** — never inline inside functions. `reports.py`, `inventory.py`, `sync.py` all had this issue.
3. **`points_base` is redundant** — `points_rate` already encodes it. Do not use `amount / points_base * points_rate`.
4. **Opening stock for date-filtered ledger** — fetched via two separate queries before `date_from`. Running stock starts from `opening_stock`, not 0.

### Frontend (App.jsx — srpc-admin-dashboard repo)
5. **Atomic ledger state** — use `setLedger({itemCode, dateFrom, dateTo})` as ONE state object. Never three separate `setState` calls — React batches renders and dates won't be ready when component mounts.
6. **`useRef` must be explicitly imported** — `import { useState, useEffect, useCallback, useRef } from "react"`. `React.useRef` doesn't work without a React import.
7. **`TagFilterDropdown`** is a thin wrapper over `MultiSelectDropdown`. `selected` array contains integer tag IDs from the API.
8. **`SortableTable`** uses `tableLayout: fixed` with `<colgroup>` — requires explicit `width` prop per column or defaults apply.

---

## API Endpoints Reference

### Reports
| Endpoint | Description |
|---|---|
| `GET /admin/reports/sales/vouchers` | Voucher-wise sales — filters: party_names, voucher_types |
| `GET /admin/reports/sales/items` | Item-wise sales — filters: tag_ids, party_names, voucher_types |
| `GET /admin/reports/purchases/vouchers` | Voucher-wise purchases — filters: supplier_names, voucher_types |
| `GET /admin/reports/purchases/items` | Item-wise purchases — filters: tag_ids, supplier_names, voucher_types |
| `GET /admin/reports/parties?search=` | Distinct party names for sales filter |
| `GET /admin/reports/suppliers?search=` | Distinct supplier names for purchase filter |

### Inventory / Ledger
| Endpoint | Description |
|---|---|
| `GET /admin/inventory/stock` | Stock summary — params: search, needs_reorder, tag_ids, sort_col, sort_dir, page |
| `GET /admin/inventory/ledger/{item_code}` | Item ledger — params: date_from, date_to (optional) |

### Sync
| Endpoint | Description |
|---|---|
| `POST /admin/sync/item-master` | Full item master sync from Google Sheet |
| `POST /admin/sync/item/{code}` | Single item sync |
| `POST /admin/sync/recalculate-points?dry_run=` | Run recalculate_points.py on server |

---

## How to Use This File

At the start of each Claude session:
1. Share this file
2. Share the specific files relevant to the task
3. Say "read CLAUDE.md first, then the files, before making any changes"

Claude should:
- Read CLAUDE.md
- Read the actual file(s) before editing
- Check the dependency map for cross-file impacts
- Verify syntax after every change
- Never assume file contents from memory