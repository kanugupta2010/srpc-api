"""
routers/sync.py
SRPC Enterprises Private Limited

Sync endpoints:
  POST /admin/sync/item-master          — Trigger full item master sync from Google Sheet
  POST /admin/sync/item/{item_code}     — Sync a single item from Google Sheet
"""

import os
import logging
import requests
import csv
import io
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from dotenv import load_dotenv

from database import get_connection
from services.dependencies import require_admin

load_dotenv()
log = logging.getLogger(__name__)

router = APIRouter(tags=["Sync"])

SHEET_ID   = os.getenv("SHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Sheet1")

# Column constants — must match Google Sheet headers exactly
COL_ALIAS               = "Alias"
COL_ITEM_NAME           = "Item_Name"
COL_PRINT_NAME          = "Print Name"
COL_HSN_CODE            = "HSN Code"
COL_GROUP               = "Group"
COL_UNIT                = "Unit"
COL_PURCHASE_EXC_GST    = "Purchase Price Exc GST"
COL_PURCHASE_INC_GST    = "Purchase Price Inc GST"
COL_SALE_EXC_GST        = "Sale Price Exc GST"
COL_SALE_INC_GST        = "Sale Price Inc GST"
COL_KACHA_SALE          = "Kacha Sale Price"
COL_BILL_LANDING        = "Bill Landing"
COL_TAX_CATEGORY        = "Tax Category"
COL_EARNS_POINTS        = "Earns Points"
COL_POINTS_RATE         = "Points Rate"
COL_REORDER_THRESHOLD   = "Reorder Threshold"
COL_DATE_OP1            = "Date - Op 1"


def _fetch_sheet_rows() -> list[dict]:
    """Fetch all rows from Google Sheet as list of dicts."""
    if not SHEET_ID:
        raise HTTPException(status_code=500, detail="SHEET_ID not configured in .env")

    url = (
        f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
        f"/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(SHEET_NAME)}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=503, detail=f"Failed to fetch sheet: {exc}")

    text = resp.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]
    return [{k.strip(): v.strip() for k, v in row.items()} for row in reader]


def _parse_decimal(raw: str):
    v = raw.strip().replace(",", "")
    if not v:
        return None
    try:
        return round(float(v), 4)
    except ValueError:
        return None


def _parse_earns_points(raw: str) -> int:
    return 1 if raw.strip().lower() in ("1", "yes", "true") else 0


def _upsert_item(cursor, row: dict) -> str:
    """Upsert a single item row. Returns 'inserted', 'updated', or 'unchanged'."""
    item_code = row.get(COL_ALIAS, "").strip()
    item_name = row.get(COL_ITEM_NAME, "").strip()
    if not item_code or not item_name:
        return "skipped"

    earns_points = _parse_earns_points(row.get(COL_EARNS_POINTS, "0"))
    points_rate  = _parse_decimal(row.get(COL_POINTS_RATE, "0")) or 0.0
    if earns_points == 0:
        points_rate = 0.0

    cursor.execute("""
        INSERT INTO item_master (
            item_code, item_name, item_print_name, category, hsn_code, unit,
            purchase_price_exc_gst, purchase_price_inc_gst,
            sale_price_exc_gst, sale_price_inc_gst,
            kacha_sale_price, bill_landing, tax_category,
            earns_points, points_rate, reorder_threshold, is_active
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1
        )
        ON DUPLICATE KEY UPDATE
            item_name               = VALUES(item_name),
            item_print_name         = VALUES(item_print_name),
            category                = VALUES(category),
            hsn_code                = VALUES(hsn_code),
            unit                    = VALUES(unit),
            purchase_price_exc_gst  = VALUES(purchase_price_exc_gst),
            purchase_price_inc_gst  = VALUES(purchase_price_inc_gst),
            sale_price_exc_gst      = VALUES(sale_price_exc_gst),
            sale_price_inc_gst      = VALUES(sale_price_inc_gst),
            kacha_sale_price        = VALUES(kacha_sale_price),
            bill_landing            = VALUES(bill_landing),
            tax_category            = VALUES(tax_category),
            earns_points            = VALUES(earns_points),
            points_rate             = VALUES(points_rate),
            reorder_threshold       = VALUES(reorder_threshold),
            is_active               = 1,
            updated_at              = CURRENT_TIMESTAMP
    """, (
        item_code[:100],
        item_name[:255],
        row.get(COL_PRINT_NAME, "").strip()[:255] or None,
        row.get(COL_GROUP, "").strip()[:100] or None,
        row.get(COL_HSN_CODE, "").strip()[:20] or None,
        row.get(COL_UNIT, "").strip()[:20] or None,
        _parse_decimal(row.get(COL_PURCHASE_EXC_GST, "")),
        _parse_decimal(row.get(COL_PURCHASE_INC_GST, "")),
        _parse_decimal(row.get(COL_SALE_EXC_GST, "")),
        _parse_decimal(row.get(COL_SALE_INC_GST, "")),
        _parse_decimal(row.get(COL_KACHA_SALE, "")),
        _parse_decimal(row.get(COL_BILL_LANDING, "")),
        row.get(COL_TAX_CATEGORY, "").strip() or None,
        earns_points,
        points_rate,
        _parse_decimal(row.get(COL_REORDER_THRESHOLD, "")) or 0,
    ))

    rc = cursor.rowcount
    if rc == 1:   return "inserted"
    if rc == 2:   return "updated"
    return "unchanged"


# ---------------------------------------------------------------------------
# POST /admin/sync/item-master — full sync
# ---------------------------------------------------------------------------

@router.post(
    "/sync/item-master",
    summary="Trigger full item master sync from Google Sheet",
)
def sync_item_master(
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    rows = _fetch_sheet_rows()
    cursor = db.cursor(dictionary=True)

    counters = dict(inserted=0, updated=0, unchanged=0, skipped=0, errors=0)
    errors = []

    for row in rows:
        try:
            result = _upsert_item(cursor, row)
            counters[result] += 1
        except Exception as exc:
            counters["errors"] += 1
            errors.append(f"{row.get(COL_ALIAS, '?')}: {exc}")

    db.commit()
    cursor.close()

    return {
        "synced_at":  datetime.utcnow().isoformat(),
        "total_rows": len(rows),
        **counters,
        "errors":     errors[:10] if errors else None,
    }


# ---------------------------------------------------------------------------
# POST /admin/sync/item/{item_code} — single item sync
# ---------------------------------------------------------------------------

@router.post(
    "/sync/item/{item_code}",
    summary="Sync a single item from Google Sheet by item code (Alias)",
)
def sync_single_item(
    item_code: str,
    payload:   dict = Depends(require_admin),
    db=Depends(get_connection),
):
    rows = _fetch_sheet_rows()

    # Find matching row by Alias
    match = next((r for r in rows if r.get(COL_ALIAS, "").strip() == item_code.strip()), None)

    if not match:
        raise HTTPException(
            status_code=404,
            detail=f"Item code '{item_code}' not found in Google Sheet."
        )

    cursor = db.cursor(dictionary=True)
    try:
        result = _upsert_item(cursor, match)
        db.commit()
    except Exception as exc:
        cursor.close()
        raise HTTPException(status_code=500, detail=str(exc))

    cursor.close()
    return {
        "item_code": item_code,
        "result":    result,
        "synced_at": datetime.utcnow().isoformat(),
    }