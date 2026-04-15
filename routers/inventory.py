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
    if sort_col == "smart_recent":
        order_clause = "latest_purchase_date DESC, item_name ASC"
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
    item_code: str,
    payload:   dict = Depends(require_admin),
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
    if not item:
        raise HTTPException(status_code=404, detail="Item not found.")

    # Purchases
    cursor.execute("""
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
        ORDER BY pi.invoice_date ASC, pi.id ASC
    """, (item_code, DEFAULT_COMPANY))
    purchases = cursor.fetchall()

    # Purchase returns
    cursor.execute("""
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
        ORDER BY pi.invoice_date ASC, pi.id ASC
    """, (item_code, DEFAULT_COMPANY))
    purchase_returns = cursor.fetchall()

    # Sales
    cursor.execute("""
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
        ORDER BY i.invoice_date ASC, i.id ASC
    """, (item_code, DEFAULT_COMPANY))
    sales = cursor.fetchall()

    # Sale returns
    cursor.execute("""
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
        ORDER BY i.invoice_date ASC, i.id ASC
    """, (item_code, DEFAULT_COMPANY))
    sale_returns = cursor.fetchall()

    cursor.close()

    # Merge and sort all transactions chronologically
    all_txns = purchases + purchase_returns + sales + sale_returns
    all_txns.sort(key=lambda x: (x["txn_date"] or "0000-00-00", x["txn_type"]))

    # Calculate running stock
    running_stock = 0.0
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

    return {
        "item":         item,
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