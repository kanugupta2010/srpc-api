"""
services/purchase_service.py
SRPC Enterprises Private Limited

Handles writing parsed purchase invoices to the database,
including tax rate lookup and price inc tax calculation.
"""

import logging
from datetime import datetime

from services.purchase_import_service import (
    ParsedPurchaseInvoice, ParsedPurchaseLine,
    INV_PURCHASE, INV_PURCHASE_RETURN,
)

log = logging.getLogger(__name__)

DEFAULT_COMPANY = "SRPC"


def _load_tax_rates(cursor, company_code: str) -> dict:
    """Returns dict: tax_category → tax_rate (float)"""
    cursor.execute(
        "SELECT tax_category, tax_rate FROM tax_rates WHERE company_code = %s",
        (company_code,)
    )
    return {r["tax_category"]: float(r["tax_rate"]) for r in cursor.fetchall()}


def _load_item_tax_categories(cursor, item_codes: list) -> dict:
    """Returns dict: item_code → tax_category"""
    if not item_codes:
        return {}
    placeholders = ",".join(["%s"] * len(item_codes))
    cursor.execute(
        f"SELECT item_code, tax_category FROM item_master WHERE item_code IN ({placeholders})",
        item_codes
    )
    return {r["item_code"]: r["tax_category"] for r in cursor.fetchall()}


def _calc_price_inc(price_exc: float, tax_rate: float) -> float:
    return round(price_exc * (1 + tax_rate / 100), 4)


def process_purchase_invoices(
    invoices: list[ParsedPurchaseInvoice],
    batch_id: int,
    db_conn,
    company_code: str = DEFAULT_COMPANY,
) -> dict:
    cursor = db_conn.cursor(dictionary=True)

    # Load tax rates once
    tax_rates     = _load_tax_rates(cursor, company_code)

    # Collect all item codes for bulk tax category lookup
    all_item_codes = list({
        line.item_code
        for inv in invoices
        for line in inv.lines
    })
    item_tax_cats = _load_item_tax_categories(cursor, all_item_codes)

    counters = dict(
        invoices_imported=0,
        invoices_duplicate=0,
        invoices_skipped=0,
        lines_imported=0,
        total_amount_inc=0.0,
        errors=0,
    )
    error_notes = []
    dates = []

    for inv in invoices:
        try:
            _process_single_purchase(
                inv, batch_id, cursor,
                tax_rates, item_tax_cats,
                company_code, counters, error_notes,
            )
            if inv.invoice_date:
                dates.append(inv.invoice_date)
        except Exception as exc:
            log.error("Error on purchase invoice %s: %s", inv.bill_number, exc)
            counters["errors"] += 1
            error_notes.append(f"{inv.bill_number or 'unknown'}: {exc}")

    # Update batch with final stats
    date_from = min(dates) if dates else None
    date_to   = max(dates) if dates else None
    cursor.execute("""
        UPDATE purchase_import_batches
        SET invoices_imported = %s,
            lines_imported    = %s,
            total_amount      = %s,
            date_from         = %s,
            date_to           = %s,
            notes             = %s
        WHERE id = %s
    """, (
        counters["invoices_imported"],
        counters["lines_imported"],
        round(counters["total_amount_inc"], 2),
        date_from, date_to,
        "; ".join(error_notes[:10]) if error_notes else None,
        batch_id,
    ))

    db_conn.commit()
    cursor.close()
    counters["notes"] = "; ".join(error_notes[:10]) if error_notes else None
    return counters


def _process_single_purchase(
    inv, batch_id, cursor,
    tax_rates, item_tax_cats,
    company_code, counters, error_notes,
):
    # Duplicate check — bill_number + type + financial_year
    if inv.bill_number:
        cursor.execute("""
            SELECT id FROM purchase_invoices
            WHERE company_code = %s
              AND bill_number = %s
              AND invoice_type = %s
              AND financial_year = %s
        """, (company_code, inv.bill_number, inv.invoice_type, inv.financial_year))
        if cursor.fetchone():
            counters["invoices_duplicate"] += 1
            counters["invoices_skipped"] += 1
            return

    # Calculate totals with tax
    line_data         = []
    gross_exc         = 0.0
    gross_inc         = 0.0

    for line in inv.lines:
        tax_cat  = item_tax_cats.get(line.item_code, "")
        tax_rate = tax_rates.get(tax_cat, 18.0)

        qty        = abs(float(line.quantity))
        # Price and Amount in Busy 21 export are INCLUSIVE of GST
        price_inc  = abs(float(line.unit_price_exc))   # field name is exc but value is inc
        amount_inc = abs(float(line.line_amount_exc))   # same — stored as inc
        # Back-calculate exc values
        price_exc  = round(price_inc / (1 + tax_rate / 100), 4)
        amount_exc = round(amount_inc / (1 + tax_rate / 100), 2)

        gross_exc += amount_exc
        gross_inc += amount_inc

        line_data.append((
            line.item_code, line.item_name,
            qty, line.unit,
            price_exc, tax_rate, price_inc,
            round(amount_exc, 2), round(amount_inc, 2),
        ))

    # Insert invoice
    cursor.execute("""
        INSERT INTO purchase_invoices (
            company_code, import_batch_id,
            invoice_date, bill_number, financial_year, supplier_name,
            invoice_type, gross_amount_exc, gross_amount_inc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        company_code, batch_id,
        inv.invoice_date, inv.bill_number, inv.financial_year, inv.supplier_name,
        inv.invoice_type,
        round(gross_exc, 2), round(gross_inc, 2),
    ))
    purchase_invoice_id = cursor.lastrowid

    # Insert lines
    if line_data:
        cursor.executemany("""
            INSERT INTO purchase_lines (
                company_code, purchase_invoice_id,
                item_code, item_name,
                quantity, unit,
                unit_price_exc, tax_rate, unit_price_inc,
                line_amount_exc, line_amount_inc
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [(company_code, purchase_invoice_id, *row) for row in line_data])

    counters["invoices_imported"] += 1
    counters["lines_imported"]    += len(line_data)
    counters["total_amount_inc"]  += gross_inc