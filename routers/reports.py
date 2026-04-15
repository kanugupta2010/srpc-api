"""
routers/reports.py
SRPC Enterprises Private Limited

Reports + Ledger endpoints:

  GET /admin/reports/sales              — Sales report with date filter
  GET /admin/reports/purchases          — Purchase report with date filter
  GET /admin/contractors/{id}/ledger    — Contractor ledger (own purchases + referred sales)
  GET /admin/customers                  — Customer list (grouped by mobile or name)
  GET /admin/customers/{key}/ledger     — Customer ledger (all invoices)
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from dateutil.relativedelta import relativedelta

from database import get_connection
from services.dependencies import require_admin

log = logging.getLogger(__name__)
router = APIRouter(tags=["Reports"])

DEFAULT_COMPANY = "SRPC"


# ---------------------------------------------------------------------------
# Date range helper
# ---------------------------------------------------------------------------

def get_date_range(period: str, date_from: Optional[str], date_to: Optional[str]):
    """
    Returns (from_date, to_date) based on period string.
    Indian Financial Year runs April–March.
    """
    today = date.today()

    def fy_start(year): return date(year, 4, 1)
    def fy_end(year):   return date(year + 1, 3, 31)

    # Current FY year (e.g. 2025 for FY 2025-26)
    fy_year = today.year if today.month >= 4 else today.year - 1

    # Current quarter start (Apr/Jul/Oct/Jan)
    q_month = ((today.month - 4) // 3) * 3 + 4
    if q_month > 12: q_month -= 12
    q_start = date(today.year if q_month <= today.month else today.year - 1, q_month, 1)
    q_end   = (q_start + relativedelta(months=3)) - timedelta(days=1)

    # Last quarter
    lq_start = q_start - relativedelta(months=3)
    lq_end   = q_start - timedelta(days=1)

    ranges = {
        "today":       (today, today),
        "yesterday":   (today - timedelta(1), today - timedelta(1)),
        "last_7":      (today - timedelta(6), today),
        "last_30":     (today - timedelta(29), today),
        "this_month":  (today.replace(day=1), today),
        "last_month":  ((today.replace(day=1) - timedelta(1)).replace(day=1), today.replace(day=1) - timedelta(1)),
        "this_quarter": (q_start, q_end),
        "last_quarter": (lq_start, lq_end),
        "current_fy":  (fy_start(fy_year), fy_end(fy_year)),
        "last_fy":     (fy_start(fy_year - 1), fy_end(fy_year - 1)),
    }

    if period == "custom":
        try:
            return (date.fromisoformat(date_from), date.fromisoformat(date_to))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid date_from or date_to for custom range.")

    if period not in ranges:
        raise HTTPException(status_code=400, detail=f"Unknown period '{period}'.")

    return ranges[period]


# ---------------------------------------------------------------------------
# GET /admin/reports/sales
# ---------------------------------------------------------------------------

@router.get("/reports/sales", summary="Sales report with date filter")
def sales_report(
    period:    str           = Query(default="this_month"),
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
    payload:   dict          = Depends(require_admin),
    db=Depends(get_connection),
):
    from_dt, to_dt = get_date_range(period, date_from, date_to)
    cursor = db.cursor(dictionary=True)

    # Summary
    cursor.execute("""
        SELECT
            COUNT(DISTINCT i.id)          AS invoice_count,
            SUM(i.gross_amount)           AS total_amount,
            SUM(i.eligible_amount)        AS eligible_amount,
            SUM(i.points_awarded)         AS points_awarded,
            COUNT(DISTINCT CASE WHEN i.invoice_type='sale_return' THEN i.id END) AS return_count,
            SUM(CASE WHEN i.invoice_type='sale_return' THEN i.gross_amount ELSE 0 END) AS return_amount
        FROM invoices i
        WHERE i.company_code = %s
          AND i.invoice_date BETWEEN %s AND %s
    """, (DEFAULT_COMPANY, from_dt, to_dt))
    summary = cursor.fetchone()

    # Daily breakdown
    cursor.execute("""
        SELECT
            i.invoice_date,
            i.invoice_type,
            COUNT(*)              AS invoice_count,
            SUM(i.gross_amount)   AS total_amount,
            SUM(i.points_awarded) AS points_awarded
        FROM invoices i
        WHERE i.company_code = %s
          AND i.invoice_date BETWEEN %s AND %s
        GROUP BY i.invoice_date, i.invoice_type
        ORDER BY i.invoice_date ASC
    """, (DEFAULT_COMPANY, from_dt, to_dt))
    daily = cursor.fetchall()

    # Invoice list
    cursor.execute("""
        SELECT
            i.id, i.invoice_date, i.bill_number, i.invoice_type,
            i.customer_type, i.party_name, i.party_mobile,
            i.referred_by_raw, i.contractor_id,
            i.gross_amount, i.eligible_amount,
            i.points_awarded, i.points_status,
            COUNT(il.id) AS line_count
        FROM invoices i
        LEFT JOIN invoice_lines il ON il.invoice_id = i.id
        WHERE i.company_code = %s
          AND i.invoice_date BETWEEN %s AND %s
        GROUP BY i.id
        ORDER BY i.invoice_date DESC, i.id DESC
    """, (DEFAULT_COMPANY, from_dt, to_dt))
    invoices = cursor.fetchall()
    cursor.close()

    return {
        "period":    period,
        "date_from": str(from_dt),
        "date_to":   str(to_dt),
        "summary":   summary,
        "daily":     daily,
        "invoices":  invoices,
    }


# ---------------------------------------------------------------------------
# GET /admin/reports/purchases
# ---------------------------------------------------------------------------

@router.get("/reports/purchases", summary="Purchase report with date filter")
def purchases_report(
    period:    str           = Query(default="this_month"),
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
    payload:   dict          = Depends(require_admin),
    db=Depends(get_connection),
):
    from_dt, to_dt = get_date_range(period, date_from, date_to)
    cursor = db.cursor(dictionary=True)

    # Summary
    cursor.execute("""
        SELECT
            COUNT(DISTINCT pi.id)              AS invoice_count,
            SUM(pi.gross_amount_inc)           AS total_amount_inc,
            SUM(pi.gross_amount_exc)           AS total_amount_exc,
            COUNT(DISTINCT CASE WHEN pi.invoice_type='purchase_return' THEN pi.id END) AS return_count,
            SUM(CASE WHEN pi.invoice_type='purchase_return' THEN pi.gross_amount_inc ELSE 0 END) AS return_amount
        FROM purchase_invoices pi
        WHERE pi.company_code = %s
          AND pi.invoice_date BETWEEN %s AND %s
    """, (DEFAULT_COMPANY, from_dt, to_dt))
    summary = cursor.fetchone()

    # Daily breakdown
    cursor.execute("""
        SELECT
            pi.invoice_date,
            pi.invoice_type,
            COUNT(*)                  AS invoice_count,
            SUM(pi.gross_amount_inc)  AS total_amount_inc
        FROM purchase_invoices pi
        WHERE pi.company_code = %s
          AND pi.invoice_date BETWEEN %s AND %s
        GROUP BY pi.invoice_date, pi.invoice_type
        ORDER BY pi.invoice_date ASC
    """, (DEFAULT_COMPANY, from_dt, to_dt))
    daily = cursor.fetchall()

    # Invoice list
    cursor.execute("""
        SELECT
            pi.id, pi.invoice_date, pi.bill_number, pi.invoice_type,
            pi.supplier_name, pi.gross_amount_inc, pi.gross_amount_exc,
            pi.financial_year,
            COUNT(pl.id) AS line_count
        FROM purchase_invoices pi
        LEFT JOIN purchase_lines pl ON pl.purchase_invoice_id = pi.id
        WHERE pi.company_code = %s
          AND pi.invoice_date BETWEEN %s AND %s
        GROUP BY pi.id
        ORDER BY pi.invoice_date DESC, pi.id DESC
    """, (DEFAULT_COMPANY, from_dt, to_dt))
    invoices = cursor.fetchall()
    cursor.close()

    return {
        "period":    period,
        "date_from": str(from_dt),
        "date_to":   str(to_dt),
        "summary":   summary,
        "daily":     daily,
        "invoices":  invoices,
    }


# ---------------------------------------------------------------------------
# GET /admin/contractors/{contractor_id}/ledger
# ---------------------------------------------------------------------------

@router.get(
    "/contractors/{contractor_id}/ledger",
    summary="Contractor ledger — own purchases + referred sales",
)
def contractor_ledger(
    contractor_id: int,
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    # Contractor info
    cursor.execute(
        "SELECT * FROM contractors WHERE id = %s AND company_code = %s",
        (contractor_id, DEFAULT_COMPANY)
    )
    contractor = cursor.fetchone()
    if not contractor:
        raise HTTPException(status_code=404, detail="Contractor not found.")

    # Sales where contractor is direct buyer (contractor_direct)
    cursor.execute("""
        SELECT
            i.invoice_date, i.bill_number, i.invoice_type,
            'own_purchase' AS ledger_type,
            i.party_name, i.party_mobile,
            i.gross_amount, i.eligible_amount,
            i.points_awarded, i.points_status,
            i.financial_year,
            il.item_code, il.item_name, il.quantity, il.unit,
            il.unit_price, il.line_amount
        FROM invoices i
        JOIN invoice_lines il ON il.invoice_id = i.id
        WHERE i.company_code = %s
          AND i.contractor_id = %s
          AND i.customer_type = 'contractor_direct'
        ORDER BY i.invoice_date DESC, i.id DESC
    """, (DEFAULT_COMPANY, contractor_id))
    own_purchases = cursor.fetchall()

    # Sales referred by this contractor (contractor_referred)
    cursor.execute("""
        SELECT
            i.invoice_date, i.bill_number, i.invoice_type,
            'referred_sale' AS ledger_type,
            i.party_name, i.party_mobile,
            i.gross_amount, i.eligible_amount,
            i.points_awarded, i.points_status,
            i.financial_year,
            il.item_code, il.item_name, il.quantity, il.unit,
            il.unit_price, il.line_amount
        FROM invoices i
        JOIN invoice_lines il ON il.invoice_id = i.id
        WHERE i.company_code = %s
          AND i.contractor_id = %s
          AND i.customer_type = 'contractor_referred'
        ORDER BY i.invoice_date DESC, i.id DESC
    """, (DEFAULT_COMPANY, contractor_id))
    referred_sales = cursor.fetchall()

    cursor.close()

    # Summary
    own_total     = sum(float(r["line_amount"] or 0) for r in own_purchases)
    referred_total = sum(float(r["line_amount"] or 0) for r in referred_sales)

    return {
        "contractor":    contractor,
        "own_purchases": own_purchases,
        "referred_sales": referred_sales,
        "summary": {
            "own_purchase_amount":  round(own_total, 2),
            "referred_sale_amount": round(referred_total, 2),
            "own_purchase_lines":   len(own_purchases),
            "referred_sale_lines":  len(referred_sales),
        }
    }


# ---------------------------------------------------------------------------
# GET /admin/customers — customer list grouped by mobile or name
# ---------------------------------------------------------------------------

@router.get("/customers", summary="Customer list grouped by mobile or party name")
def list_customers(
    search:    Optional[str] = Query(default=None),
    page:      int           = Query(default=1, ge=1),
    page_size: int           = Query(default=50, ge=1, le=200),
    payload:   dict          = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    search_clause = ""
    params = [DEFAULT_COMPANY]
    if search:
        search_clause = "AND (party_name LIKE %s OR party_mobile LIKE %s)"
        params.extend([f"%{search}%", f"%{search}%"])

    # Group by mobile where available, else by party_name
    cursor.execute(f"""
        SELECT
            COALESCE(NULLIF(party_mobile,''), party_name)  AS customer_key,
            MAX(party_name)                                AS party_name,
            MAX(party_mobile)                              AS party_mobile,
            COUNT(DISTINCT id)                             AS invoice_count,
            SUM(gross_amount)                              AS total_amount,
            MAX(invoice_date)                              AS last_transaction,
            MIN(invoice_date)                              AS first_transaction,
            SUM(CASE WHEN invoice_type='sale' THEN gross_amount ELSE 0 END) AS sales_amount,
            SUM(CASE WHEN invoice_type='sale_return' THEN gross_amount ELSE 0 END) AS returns_amount
        FROM invoices
        WHERE company_code = %s {search_clause}
        GROUP BY customer_key
        ORDER BY last_transaction DESC
        LIMIT %s OFFSET %s
    """, params + [page_size, (page - 1) * page_size])
    customers = cursor.fetchall()

    cursor.execute(f"""
        SELECT COUNT(DISTINCT COALESCE(NULLIF(party_mobile,''), party_name)) AS total
        FROM invoices
        WHERE company_code = %s {search_clause}
    """, params[:-2] if search else params)
    total = cursor.fetchone()["total"]

    # Also include purchase suppliers
    cursor.execute(f"""
        SELECT
            supplier_name                  AS customer_key,
            supplier_name                  AS party_name,
            NULL                           AS party_mobile,
            COUNT(DISTINCT id)             AS invoice_count,
            SUM(gross_amount_inc)          AS total_amount,
            MAX(invoice_date)              AS last_transaction,
            MIN(invoice_date)              AS first_transaction,
            SUM(CASE WHEN invoice_type='purchase' THEN gross_amount_inc ELSE 0 END) AS sales_amount,
            SUM(CASE WHEN invoice_type='purchase_return' THEN gross_amount_inc ELSE 0 END) AS returns_amount
        FROM purchase_invoices
        WHERE company_code = %s
        {"AND supplier_name LIKE %s" if search else ""}
        GROUP BY supplier_name
        ORDER BY last_transaction DESC
        LIMIT %s OFFSET %s
    """, ([DEFAULT_COMPANY, f"%{search}%", page_size, (page-1)*page_size] if search
          else [DEFAULT_COMPANY, page_size, (page-1)*page_size]))
    suppliers = cursor.fetchall()

    cursor.close()
    return {
        "page": page, "page_size": page_size,
        "total": total,
        "customers": customers,
        "suppliers": suppliers,
    }


# ---------------------------------------------------------------------------
# GET /admin/customers/{key}/ledger
# ---------------------------------------------------------------------------

@router.get("/customers/{customer_key}/ledger", summary="Customer ledger — all invoices")
def customer_ledger(
    customer_key: str,
    payload: dict = Depends(require_admin),
    db=Depends(get_connection),
):
    cursor = db.cursor(dictionary=True)

    # Sales invoices — match by mobile or name
    cursor.execute("""
        SELECT
            i.invoice_date, i.bill_number, i.invoice_type,
            'sale' AS source,
            i.party_name, i.party_mobile,
            i.gross_amount AS amount,
            i.financial_year,
            il.item_code, il.item_name,
            il.quantity, il.unit,
            il.unit_price, il.line_amount
        FROM invoices i
        JOIN invoice_lines il ON il.invoice_id = i.id
        WHERE i.company_code = %s
          AND (
              (i.party_mobile = %s AND i.party_mobile != '')
              OR (COALESCE(NULLIF(i.party_mobile,''), i.party_name) = %s)
          )
        ORDER BY i.invoice_date DESC, i.id DESC
    """, (DEFAULT_COMPANY, customer_key, customer_key))
    sale_lines = cursor.fetchall()

    # Purchase invoices — match by supplier name
    cursor.execute("""
        SELECT
            pi.invoice_date, pi.bill_number, pi.invoice_type,
            'purchase' AS source,
            pi.supplier_name AS party_name,
            NULL AS party_mobile,
            pi.gross_amount_inc AS amount,
            pi.financial_year,
            pl.item_code, pl.item_name,
            pl.quantity, pl.unit,
            pl.unit_price_inc AS unit_price,
            pl.line_amount_inc AS line_amount
        FROM purchase_invoices pi
        JOIN purchase_lines pl ON pl.purchase_invoice_id = pi.id
        WHERE pi.company_code = %s
          AND pi.supplier_name = %s
        ORDER BY pi.invoice_date DESC, pi.id DESC
    """, (DEFAULT_COMPANY, customer_key))
    purchase_lines = cursor.fetchall()
    cursor.close()

    all_lines = sale_lines + purchase_lines
    all_lines.sort(key=lambda x: (x["invoice_date"] or date.min), reverse=True)

    total_sales    = sum(float(r["line_amount"] or 0) for r in sale_lines)
    total_purchases = sum(float(r["line_amount"] or 0) for r in purchase_lines)

    return {
        "customer_key": customer_key,
        "party_name":   sale_lines[0]["party_name"] if sale_lines else purchase_lines[0]["party_name"] if purchase_lines else customer_key,
        "transactions": all_lines,
        "summary": {
            "total_sale_amount":     round(total_sales, 2),
            "total_purchase_amount": round(total_purchases, 2),
            "sale_lines":            len(sale_lines),
            "purchase_lines":        len(purchase_lines),
        }
    }