"""
routers/inventory.py
SRPC Enterprises Private Limited

Inventory and purchase management endpoints (admin only):

  POST /admin/purchases/import          — Upload purchase XLSX/CSV
  GET  /admin/purchases                 — List purchase batches
  GET  /admin/purchases/{batch_id}      — Batch detail + invoices

  GET  /admin/inventory/stock           — Stock summary for all items
  GET  /admin/inventory/stock/{item_code} — Single item stock detail
  PUT  /admin/inventory/threshold/{item_code} — Set reorder threshold
  GET  /admin/inventory/reorder         — Items below reorder threshold
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from pydantic import BaseModel

from database import get_connection
import re
from services.dependencies import require_admin
from services.purchase_import_service import parse_purchase_file
from services.purchase_service import process_purchase_invoices

log = logging.getLogger(__name__)

router = APIRouter(tags=["Inventory"])

DEFAULT_COMPANY = "SRPC"


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ThresholdRequest(BaseModel):
    reorder_threshold: float


class ImportResponse(BaseModel):
    batch_id:           int
    invoices_imported:  int
    invoices_duplicate: int
    lines_imported:     int
    total_amount_inc:   float
    date_from:          Optional[str]
    date_to:            Optional[str]
    notes:              Optional[str]


# ---------------------------------------------------------------------------
# POST /admin/purchases/import
# ---------------------------------------------------------------------------

@router.post(
    "/purchases/import",
    response_model=ImportResponse,
    summary="Import purchase register XLSX or CSV from Busy 21",
)
async def import_purchases(
    file: UploadFile = File(...),
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    try:
        invoices, stats = parse_purchase_file(content, file.filename)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    if not invoices:
        raise HTTPException(status_code=422, detail="No invoices found in file.")

    cursor = db.cursor(dictionary=True)

    # Create import batch
    cursor.execute("""
        INSERT INTO purchase_import_batches (company_code, filename, imported_by)
        VALUES (%s, %s, %s)
    """, (DEFAULT_COMPANY, file.filename, payload.get("sub", "admin")))
    batch_id = cursor.lastrowid
    db.commit()
    cursor.close()

    counters = process_purchase_invoices(invoices, batch_id, db, DEFAULT_COMPANY)

    # Fetch final batch record for dates
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM purchase_import_batches WHERE id = %s", (batch_id,))
    batch = cursor.fetchone()
    cursor.close()

    return {
        "batch_id":           batch_id,
        "invoices_imported":  counters["invoices_imported"],
        "invoices_duplicate": counters["invoices_duplicate"],
        "lines_imported":     counters["lines_imported"],
        "total_amount_inc":   round(counters["total_amount_inc"], 2),
        "date_from":          str(batch["date_from"]) if batch["date_from"] else None,
        "date_to":            str(batch["date_to"]) if batch["date_to"] else None,
        "notes":              counters.get("notes"),
    }


# ---------------------------------------------------------------------------
# GET /admin/purchases
# ---------------------------------------------------------------------------

@router.get(
    "/purchases",
    summary="List all purchase import batches",
)
def list_purchase_batches(
    page:      int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    payload:   dict = Depends(require_admin),
    db=Depends(get_connection),
):
    offset = (page - 1) * page_size
    cursor = db.cursor(dictionary=True)

    cursor.execute(
        "SELECT COUNT(*) AS total FROM purchase_import_batches WHERE company_code = %s",
        (DEFAULT_COMPANY,)
    )
    total = cursor.fetchone()["total"]

    cursor.execute("""
        SELECT id, filename, imported_by, invoices_imported, lines_imported,
               total_amount, date_from, date_to, created_at
        FROM purchase_import_batches
        WHERE company_code = %s
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, (DEFAULT_COMPANY, page_size, offset))
    batches = cursor.fetchall()
    cursor.close()

    return {"page": page, "page_size": page_size, "total": total, "batches": batches}


# ---------------------------------------------------------------------------
# GET /admin/purchases/{batch_id}
# ---------------------------------------------------------------------------

@router.get(
    "/purchases/{batch_id}",
    summary="Get purchase batch detail with all invoices",
)
def get_purchase_batch(
    batch_id: int,
    payload:  dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    cursor.execute(
        "SELECT * FROM purchase_import_batches WHERE id = %s AND company_code = %s",
        (batch_id, DEFAULT_COMPANY)
    )
    batch = cursor.fetchone()
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found.")

    cursor.execute("""
        SELECT pi.id, pi.invoice_date, pi.bill_number, pi.supplier_name,
               pi.invoice_type, pi.gross_amount_exc, pi.gross_amount_inc,
               COUNT(pl.id) AS line_count
        FROM purchase_invoices pi
        LEFT JOIN purchase_lines pl ON pl.purchase_invoice_id = pi.id
        WHERE pi.import_batch_id = %s AND pi.company_code = %s
        GROUP BY pi.id
        ORDER BY pi.invoice_date ASC, pi.id ASC
    """, (batch_id, DEFAULT_COMPANY))
    invoices = cursor.fetchall()
    cursor.close()

    return {"batch": batch, "invoices": invoices}


# ---------------------------------------------------------------------------
# GET /admin/inventory/stock
# ---------------------------------------------------------------------------

@router.get(
    "/inventory/stock",
    summary="Stock summary for all items",
)
def get_stock_summary(
    search:       Optional[str]  = Query(default=None, description="Search item name or code"),
    needs_reorder: Optional[bool] = Query(default=None, description="Filter items needing reorder"),
    category:     Optional[str]  = Query(default=None, description="Filter by category"),
    tag_ids:      Optional[str]  = Query(default=None, description="Comma-separated tag IDs to filter by"),
    sort_col:     Optional[str]  = Query(default="needs_reorder", description="Column to sort by"),
    sort_dir:     Optional[str]  = Query(default="desc", description="asc or desc"),
    page:         int = Query(default=1, ge=1),
    page_size:    int = Query(default=50, ge=1, le=500),
    payload:      dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    where = ["company_code = %s"]
    params = [DEFAULT_COMPANY]

    if search:
        # Multi-keyword search — "AP Black Enamel" matches items containing ALL words
        keywords = [w for w in search.strip().split() if w]
        for kw in keywords:
            like = f"%{kw}%"
            where.append("(item_code LIKE %s OR item_name LIKE %s OR item_print_name LIKE %s OR category LIKE %s)")
            params.extend([like, like, like, like])
    if needs_reorder is True:
        where.append("needs_reorder = 1")
    elif needs_reorder is False:
        where.append("needs_reorder = 0")
    if category:
        where.append("category = %s")
        params.append(category)
    if tag_ids:
        ids = [i.strip() for i in tag_ids.split(",") if i.strip().isdigit()]
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            where.append(f"item_code IN (SELECT item_code FROM item_tag_map WHERE tag_id IN ({placeholders}) AND company_code = %s)")
            params.extend(ids)
            params.append(DEFAULT_COMPANY)

    where_clause = " AND ".join(where)

    # Total count
    cursor.execute(
        f"SELECT COUNT(*) AS total FROM vw_stock_summary WHERE {where_clause}",
        params
    )
    total = cursor.fetchone()["total"]

    # Paginated results
    offset = (page - 1) * page_size
    # Safe column whitelist — prevents SQL injection
    SORTABLE = {
        "item_code", "item_name", "category", "unit",
        "current_stock", "qty_sold", "qty_purchased",
        "latest_purchase_price_inc", "bill_landing",
        "reorder_threshold", "needs_reorder", "latest_purchase_date",
    }
    # Smart sort options
    if sort_col == "smart_activity":
        order_clause = "GREATEST(COALESCE(latest_purchase_date,'1970-01-01'), COALESCE(latest_sale_date,'1970-01-01')) DESC, item_name ASC"
    elif sort_col == "smart_purchased":
        order_clause = "latest_purchase_date DESC, item_name ASC"
    elif sort_col == "smart_sold_date":
        order_clause = "latest_sale_date DESC, item_name ASC"
    elif sort_col == "smart_sold":
        order_clause = "qty_sold DESC, item_name ASC"
    elif sort_col in SORTABLE:
        direction = "DESC" if sort_dir == "desc" else "ASC"
        order_clause = f"{sort_col} {direction}, item_name ASC"
    else:
        order_clause = "needs_reorder DESC, item_name ASC"

    cursor.execute(f"""
        SELECT
            item_code, item_name, item_print_name, category, unit,
            qty_purchased, qty_purchase_returned,
            qty_sold, qty_sale_returned,
            current_stock,
            latest_purchase_price_exc,
            latest_purchase_price_inc,
            latest_purchase_date,
            latest_sale_date,
            bill_landing,
            reorder_threshold,
            needs_reorder
        FROM vw_stock_summary
        WHERE {where_clause}
        ORDER BY {order_clause}
        LIMIT %s OFFSET %s
    """, params + [page_size, offset])
    items = cursor.fetchall()

    # Summary counts
    cursor.execute(
        "SELECT COUNT(*) AS reorder_count FROM vw_stock_summary WHERE company_code = %s AND needs_reorder = 1",
        (DEFAULT_COMPANY,)
    )
    reorder_count = cursor.fetchone()["reorder_count"]
    cursor.close()

    return {
        "page":          page,
        "page_size":     page_size,
        "total":         total,
        "reorder_count": reorder_count,
        "items":         items,
    }


# ---------------------------------------------------------------------------
# GET /admin/inventory/stock/{item_code}
# ---------------------------------------------------------------------------

@router.get(
    "/inventory/stock/{item_code}",
    summary="Single item stock detail with purchase history",
)
def get_item_stock_detail(
    item_code: str,
    payload:   dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    cursor.execute(
        "SELECT * FROM vw_stock_summary WHERE company_code = %s AND item_code = %s",
        (DEFAULT_COMPANY, item_code)
    )
    stock = cursor.fetchone()
    if not stock:
        raise HTTPException(status_code=404, detail="Item not found.")

    # Purchase history for this item
    cursor.execute("""
        SELECT pi.invoice_date, pi.bill_number, pi.supplier_name, pi.invoice_type,
               pl.quantity, pl.unit, pl.unit_price_exc, pl.unit_price_inc,
               pl.line_amount_exc, pl.line_amount_inc, pl.tax_rate
        FROM purchase_lines pl
        JOIN purchase_invoices pi ON pi.id = pl.purchase_invoice_id
        WHERE pl.item_code = %s AND pl.company_code = %s
        ORDER BY pi.invoice_date DESC, pi.id DESC
        LIMIT 50
    """, (item_code, DEFAULT_COMPANY))
    purchase_history = cursor.fetchall()

    # Sales history for this item
    cursor.execute("""
        SELECT i.invoice_date, i.bill_number, i.invoice_type, i.customer_type,
               il.quantity, il.unit, il.unit_price, il.line_amount
        FROM invoice_lines il
        JOIN invoices i ON i.id = il.invoice_id
        WHERE il.item_code = %s AND i.company_code = %s
        ORDER BY i.invoice_date DESC, i.id DESC
        LIMIT 50
    """, (item_code, DEFAULT_COMPANY))
    sales_history = cursor.fetchall()

    cursor.close()

    return {
        "stock":            stock,
        "purchase_history": purchase_history,
        "sales_history":    sales_history,
    }


# ---------------------------------------------------------------------------
# PUT /admin/inventory/threshold/{item_code}
# ---------------------------------------------------------------------------

@router.put(
    "/inventory/threshold/{item_code}",
    summary="Set reorder threshold for an item",
)
def set_reorder_threshold(
    item_code: str,
    req:       ThresholdRequest,
    payload:   dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    # Verify item exists
    cursor.execute(
        "SELECT item_code FROM item_master WHERE item_code = %s AND is_active = 1",
        (item_code,)
    )
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="Item not found in item_master.")

    cursor.execute("""
        UPDATE item_master
        SET reorder_threshold = %s
        WHERE item_code = %s AND company_code = %s
    """, (req.reorder_threshold, item_code, DEFAULT_COMPANY))
    db.commit()
    cursor.close()

    return {"message": f"Threshold updated for {item_code}", "reorder_threshold": req.reorder_threshold}


# ---------------------------------------------------------------------------
# GET /admin/inventory/reorder
# ---------------------------------------------------------------------------

@router.get(
    "/inventory/reorder",
    summary="Items currently below reorder threshold",
)
def get_reorder_items(
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT item_code, item_name, item_print_name, category, unit,
               current_stock, reorder_threshold,
               latest_purchase_price_inc, latest_purchase_date
        FROM vw_stock_summary
        WHERE company_code = %s AND needs_reorder = 1
        ORDER BY (current_stock / reorder_threshold) ASC
    """, (DEFAULT_COMPANY,))
    items = cursor.fetchall()
    cursor.close()

    return {"count": len(items), "items": items}


# ---------------------------------------------------------------------------
# GET /admin/inventory/ledger/{item_code} — full item ledger
# ---------------------------------------------------------------------------

@router.get(
    "/inventory/ledger/{item_code}",
    summary="Full chronological ledger for an item — purchases, sales, returns",
)
def get_item_ledger(
    item_code:  str,
    date_from:  Optional[str] = Query(default=None, description="Filter from date YYYY-MM-DD"),
    date_to:    Optional[str] = Query(default=None, description="Filter to date YYYY-MM-DD"),
    payload:    dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    # Item details
    cursor.execute("""
        SELECT item_code, item_name, item_print_name, category, unit,
               bill_landing, reorder_threshold, earns_points, points_rate
        FROM item_master WHERE item_code = %s AND is_active = 1
    """, (item_code,))
    item = cursor.fetchone()
    # Don't 404 — item may exist in transactions but not item_master
    # Return null item and still fetch ledger transactions

    # Build optional date filter
    date_clause_pi = ""
    date_clause_i  = ""
    date_params    = []
    if date_from and date_to:
        date_clause_pi = "AND pi.invoice_date BETWEEN %s AND %s"
        date_clause_i  = "AND i.invoice_date BETWEEN %s AND %s"
        date_params    = [date_from, date_to]

    def run(q, params): cursor.execute(q, params); return cursor.fetchall()

    # Purchases
    purchases = run(f"""
        SELECT
            pi.invoice_date    AS txn_date,
            'purchase'         AS txn_type,
            pi.bill_number,
            pi.supplier_name   AS party,
            NULL               AS party_mobile,
            pl.quantity,
            pl.unit,
            pl.unit_price_inc  AS price,
            pl.line_amount_inc AS amount,
            pi.financial_year
        FROM purchase_lines pl
        JOIN purchase_invoices pi ON pi.id = pl.purchase_invoice_id
        WHERE pl.item_code = %s AND pl.company_code = %s
          AND pi.invoice_type = 'purchase'
          {date_clause_pi}
        ORDER BY pi.invoice_date DESC, pi.id DESC
    """, [item_code, DEFAULT_COMPANY] + date_params)

    # Purchase returns
    purchase_returns = run(f"""
        SELECT
            pi.invoice_date     AS txn_date,
            'purchase_return'   AS txn_type,
            pi.bill_number,
            pi.supplier_name    AS party,
            NULL                AS party_mobile,
            pl.quantity,
            pl.unit,
            pl.unit_price_inc   AS price,
            pl.line_amount_inc  AS amount,
            pi.financial_year
        FROM purchase_lines pl
        JOIN purchase_invoices pi ON pi.id = pl.purchase_invoice_id
        WHERE pl.item_code = %s AND pl.company_code = %s
          AND pi.invoice_type = 'purchase_return'
          {date_clause_pi}
        ORDER BY pi.invoice_date DESC, pi.id DESC
    """, [item_code, DEFAULT_COMPANY] + date_params)

    # Sales
    sales = run(f"""
        SELECT
            i.invoice_date  AS txn_date,
            'sale'          AS txn_type,
            i.bill_number,
            i.party_name    AS party,
            i.party_mobile,
            il.quantity,
            il.unit,
            il.unit_price   AS price,
            il.line_amount  AS amount,
            i.financial_year
        FROM invoice_lines il
        JOIN invoices i ON i.id = il.invoice_id
        WHERE il.item_code = %s AND i.company_code = %s
          AND i.invoice_type = 'sale'
          {date_clause_i}
        ORDER BY i.invoice_date DESC, i.id DESC
    """, [item_code, DEFAULT_COMPANY] + date_params)

    # Sale returns
    sale_returns = run(f"""
        SELECT
            i.invoice_date  AS txn_date,
            'sale_return'   AS txn_type,
            i.bill_number,
            i.party_name    AS party,
            i.party_mobile,
            il.quantity,
            il.unit,
            il.unit_price   AS price,
            il.line_amount  AS amount,
            i.financial_year
        FROM invoice_lines il
        JOIN invoices i ON i.id = il.invoice_id
        WHERE il.item_code = %s AND i.company_code = %s
          AND i.invoice_type = 'sale_return'
          {date_clause_i}
        ORDER BY i.invoice_date DESC, i.id DESC
    """, [item_code, DEFAULT_COMPANY] + date_params)

    # Merge and sort all transactions chronologically
    all_txns = purchases + purchase_returns + sales + sale_returns
    # Sort ASC first to calculate running stock correctly (oldest → newest)
    all_txns.sort(key=lambda x: (x["txn_date"] or "0000-00-00", x["txn_type"]))

    # If date-filtered, fetch opening stock (stock before date_from)
    opening_stock = 0.0
    if not date_from:
        cursor.close()
    if date_from:
        # Sum all transactions strictly before date_from
        cursor.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN pi.invoice_type='purchase' THEN pl.quantity ELSE 0 END), 0)
                - COALESCE(SUM(CASE WHEN pi.invoice_type='purchase_return' THEN ABS(pl.quantity) ELSE 0 END), 0)
                AS net_purchased
            FROM purchase_lines pl
            JOIN purchase_invoices pi ON pi.id = pl.purchase_invoice_id
            WHERE pl.item_code = %s AND pl.company_code = %s
              AND pi.invoice_date < %s
        """, (item_code, DEFAULT_COMPANY, date_from))
        row = cursor.fetchone()
        purchase_opening = float((row or {}).get("net_purchased") or 0)

        cursor.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN i.invoice_type='sale' THEN ABS(il.quantity) ELSE 0 END), 0)
                - COALESCE(SUM(CASE WHEN i.invoice_type='sale_return' THEN ABS(il.quantity) ELSE 0 END), 0)
                AS net_sold
            FROM invoice_lines il
            JOIN invoices i ON i.id = il.invoice_id
            WHERE il.item_code = %s AND i.company_code = %s
              AND i.invoice_date < %s
        """, (item_code, DEFAULT_COMPANY, date_from))
        row = cursor.fetchone()
        sale_opening = float((row or {}).get("net_sold") or 0)

        opening_stock = round(purchase_opening - sale_opening, 4)

    cursor.close()

    # Calculate running stock in chronological order, starting from opening_stock
    running_stock = opening_stock
    for txn in all_txns:
        qty = float(txn["quantity"] or 0)
        if txn["txn_type"] == "purchase":
            running_stock += qty
        elif txn["txn_type"] == "purchase_return":
            running_stock -= abs(qty)
        elif txn["txn_type"] == "sale":
            running_stock -= abs(qty)
        elif txn["txn_type"] == "sale_return":
            running_stock += abs(qty)
        txn["running_stock"] = round(running_stock, 4)

    # Reverse to DESC for display (most recent first)
    all_txns.reverse()

    return {
        "item":         item,
        "date_from":    date_from,
        "date_to":      date_to,
        "opening_stock": opening_stock,
        "transactions": all_txns,
        "summary": {
            "total_purchased":       sum(float(t["quantity"] or 0) for t in purchases),
            "total_purchase_returned": sum(abs(float(t["quantity"] or 0)) for t in purchase_returns),
            "total_sold":            sum(abs(float(t["quantity"] or 0)) for t in sales),
            "total_sale_returned":   sum(abs(float(t["quantity"] or 0)) for t in sale_returns),
            "current_stock":         round(running_stock, 4),
        }
    }


# ---------------------------------------------------------------------------
# PUT /admin/inventory/bill-landing/{item_code} — update bill landing price
# ---------------------------------------------------------------------------

class BillLandingRequest(BaseModel):
    bill_landing: Optional[float]


@router.put(
    "/inventory/bill-landing/{item_code}",
    summary="Update bill landing price for an item",
)
def update_bill_landing(
    item_code: str,
    req:       BillLandingRequest,
    payload:   dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        "SELECT item_code FROM item_master WHERE item_code = %s AND is_active = 1",
        (item_code,)
    )
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="Item not found.")

    cursor.execute(
        "UPDATE item_master SET bill_landing = %s WHERE item_code = %s",
        (req.bill_landing, item_code)
    )
    db.commit()
    cursor.close()
    return {"message": f"Bill landing updated for {item_code}", "bill_landing": req.bill_landing}


# ---------------------------------------------------------------------------
# Tags endpoints
# ---------------------------------------------------------------------------

# GET /admin/inventory/tags — list all tags with item counts
@router.get("/inventory/tags", summary="List all tags with item counts and purchase totals")
def list_tags(
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            t.id,
            t.tag_name,
            t.description,
            COUNT(DISTINCT m.item_code) AS item_count,
            t.created_at
        FROM item_tags t
        LEFT JOIN item_tag_map m ON m.tag_id = t.id AND m.company_code = t.company_code
        WHERE t.company_code = %s
        GROUP BY t.id
        ORDER BY t.tag_name ASC
    """, (DEFAULT_COMPANY,))
    tags = cursor.fetchall()
    cursor.close()
    return {"tags": tags}


# GET /admin/inventory/tags/{tag_id}/items — items under a tag
@router.get("/inventory/tags/{tag_id}/items", summary="Get all items for a tag")
def get_tag_items(
    tag_id:  int,
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM item_tags WHERE id = %s AND company_code = %s",
        (tag_id, DEFAULT_COMPANY)
    )
    tag = cursor.fetchone()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found.")

    cursor.execute("""
        SELECT im.item_code, im.item_name, im.item_print_name,
               im.category, im.unit, im.actual_quantity,
               im.bill_landing, im.reorder_threshold
        FROM item_tag_map m
        JOIN item_master im ON im.item_code = m.item_code
        WHERE m.tag_id = %s AND m.company_code = %s AND im.is_active = 1
        ORDER BY im.item_name ASC
    """, (tag_id, DEFAULT_COMPANY))
    items = cursor.fetchall()
    cursor.close()
    return {"tag": tag, "items": items}


# GET /admin/inventory/tags/{tag_id}/report — purchase/sale report for a tag
@router.get("/inventory/tags/{tag_id}/report", summary="Purchase and sale totals for a tag with date filter")
def get_tag_report(
    tag_id:    int,
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
    payload:   dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM item_tags WHERE id = %s AND company_code = %s",
        (tag_id, DEFAULT_COMPANY)
    )
    tag = cursor.fetchone()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found.")

    date_filter = ""
    params_base = [tag_id, DEFAULT_COMPANY]
    if date_from and date_to:
        date_filter = "AND pi.invoice_date BETWEEN %s AND %s"
        params_base += [date_from, date_to]

    # Purchase totals (using actual_quantity weight if available)
    cursor.execute(f"""
        SELECT
            im.item_code,
            im.item_name,
            im.unit,
            im.actual_quantity,
            SUM(pl.quantity)                AS qty_purchased,
            SUM(pl.line_amount_inc)         AS amount_inc,
            MAX(pi.invoice_date)            AS last_purchased
        FROM item_tag_map m
        JOIN item_master im       ON im.item_code = m.item_code
        JOIN purchase_lines pl    ON pl.item_code = m.item_code AND pl.company_code = m.company_code
        JOIN purchase_invoices pi ON pi.id = pl.purchase_invoice_id
        WHERE m.tag_id = %s AND m.company_code = %s
          AND pi.invoice_type = 'purchase'
          {date_filter}
        GROUP BY im.item_code, im.item_name, im.unit, im.actual_quantity
        ORDER BY qty_purchased DESC
    """, params_base)
    purchase_items = cursor.fetchall()

    # Sales totals
    params_sale = [tag_id, DEFAULT_COMPANY]
    date_filter_sale = ""
    if date_from and date_to:
        date_filter_sale = "AND i.invoice_date BETWEEN %s AND %s"
        params_sale += [date_from, date_to]

    cursor.execute(f"""
        SELECT
            im.item_code,
            im.item_name,
            im.unit,
            im.actual_quantity,
            SUM(ABS(il.quantity))   AS qty_sold,
            SUM(ABS(il.line_amount)) AS amount,
            MAX(i.invoice_date)      AS last_sold
        FROM item_tag_map m
        JOIN item_master im   ON im.item_code = m.item_code
        JOIN invoice_lines il ON il.item_code = m.item_code
        JOIN invoices i       ON i.id = il.invoice_id AND i.company_code = m.company_code
        WHERE m.tag_id = %s AND m.company_code = %s
          AND i.invoice_type = 'sale'
          {date_filter_sale}
        GROUP BY im.item_code, im.item_name, im.unit, im.actual_quantity
        ORDER BY qty_sold DESC
    """, params_sale)
    sale_items = cursor.fetchall()

    cursor.close()

    total_purchased = sum(float(r["qty_purchased"] or 0) for r in purchase_items)
    total_sold      = sum(float(r["qty_sold"] or 0) for r in sale_items)
    total_amt_inc   = sum(float(r["amount_inc"] or 0) for r in purchase_items)
    total_sale_amt  = sum(float(r["amount"] or 0) for r in sale_items)

    def _unit_totals(items: list, qty_key: str) -> dict:
        """
        Group by unit type parsed from actual_quantity.
        e.g. actual_quantity='18lt' → unit_type='lt', volume=18.0
        Returns dict: unit_type → {total_volume, item_count, unit_qty_total}
        """
        totals: dict = {}
        for row in items:
            aq  = (row.get("actual_quantity") or "").strip().lower()
            qty = float(row.get(qty_key) or 0)
            m   = re.match(r'^([\d.]+)([a-z]+)$', aq)
            if m:
                vol      = float(m.group(1))
                unit_type = m.group(2)   # lt, ml, kg, etc.
                total_vol = round(vol * qty, 4)
            else:
                unit_type = row.get("unit") or "pcs"
                total_vol = qty
            if unit_type not in totals:
                totals[unit_type] = {"total_volume": 0.0, "item_count": 0, "unit_qty": 0.0}
            totals[unit_type]["total_volume"] = round(totals[unit_type]["total_volume"] + total_vol, 4)
            totals[unit_type]["item_count"]  += 1
            totals[unit_type]["unit_qty"]     = round(totals[unit_type]["unit_qty"] + qty, 4)
        return totals

    purchase_unit_totals = _unit_totals(purchase_items, "qty_purchased")
    sale_unit_totals     = _unit_totals(sale_items, "qty_sold")

    return {
        "tag":            tag,
        "date_from":      date_from,
        "date_to":        date_to,
        "purchase_items": purchase_items,
        "sale_items":     sale_items,
        "purchase_unit_totals": purchase_unit_totals,
        "sale_unit_totals":     sale_unit_totals,
        "summary": {
            "total_qty_purchased":    round(total_purchased, 4),
            "total_qty_sold":         round(total_sold, 4),
            "total_amount_inc":       round(total_amt_inc, 2),
            "total_sale_amount":      round(total_sale_amt, 2),
            "item_count":             len(set(r["item_code"] for r in purchase_items + sale_items)),
            "purchase_unit_totals":   purchase_unit_totals,
            "sale_unit_totals":       sale_unit_totals,
        }
    }


# POST /admin/inventory/tags — create a new tag
@router.post("/inventory/tags", summary="Create a new tag")
def create_tag(
    req:     dict,
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    tag_name    = (req.get("tag_name") or "").strip()
    description = (req.get("description") or "").strip() or None
    if not tag_name:
        raise HTTPException(status_code=400, detail="tag_name is required.")
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute(
            "INSERT INTO item_tags (company_code, tag_name, description) VALUES (%s, %s, %s)",
            (DEFAULT_COMPANY, tag_name, description)
        )
        tag_id = cursor.lastrowid
        db.commit()
    except Exception:
        raise HTTPException(status_code=409, detail=f"Tag '{tag_name}' already exists.")
    cursor.close()
    return {"id": tag_id, "tag_name": tag_name, "description": description}


# POST /admin/inventory/tags/bulk-assign — assign tag to items matching a pattern
@router.post("/inventory/tags/bulk-assign", summary="Bulk assign a tag to items matching a name pattern")
def bulk_assign_tag(
    req:     dict,
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    tag_id  = req.get("tag_id")
    pattern = (req.get("pattern") or "").strip()
    if not tag_id or not pattern:
        raise HTTPException(status_code=400, detail="tag_id and pattern are required.")

    cursor = db.cursor(dictionary=True)

    # Verify tag exists
    cursor.execute("SELECT id FROM item_tags WHERE id = %s AND company_code = %s", (tag_id, DEFAULT_COMPANY))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="Tag not found.")

    # Find matching items
    cursor.execute("""
        SELECT item_code FROM item_master
        WHERE company_code = %s AND is_active = 1
          AND (item_name LIKE %s OR item_print_name LIKE %s OR tags_raw LIKE %s)
    """, (DEFAULT_COMPANY, f"%{pattern}%", f"%{pattern}%", f"%{pattern}%"))
    items = cursor.fetchall()

    if not items:
        cursor.close()
        return {"message": "No items matched the pattern.", "assigned": 0}

    cursor.executemany("""
        INSERT IGNORE INTO item_tag_map (company_code, item_code, tag_id)
        VALUES (%s, %s, %s)
    """, [(DEFAULT_COMPANY, r["item_code"], tag_id) for r in items])
    db.commit()
    cursor.close()
    return {"message": f"Tag assigned to {len(items)} items.", "assigned": len(items)}


# DELETE /admin/inventory/tags/{tag_id}/items/{item_code} — remove tag from item
@router.delete(
    "/inventory/tags/{tag_id}/items/{item_code}",
    summary="Remove a tag from a specific item",
)
def remove_item_tag(
    tag_id:    int,
    item_code: str,
    payload:   dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor()
    cursor.execute(
        "DELETE FROM item_tag_map WHERE company_code = %s AND tag_id = %s AND item_code = %s",
        (DEFAULT_COMPANY, tag_id, item_code)
    )
    db.commit()
    cursor.close()
    return {"message": f"Tag removed from {item_code}."}