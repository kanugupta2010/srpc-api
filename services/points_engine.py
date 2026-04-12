"""
services/points_engine.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Points engine — calculates points per invoice and writes to:
  - invoices
  - invoice_lines
  - points_log  (immutable — INSERT only, never UPDATE/DELETE)
  - contractors (balance + tier update)

Called once per import batch after contractor resolution.
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Optional

from services.import_service import ParsedInvoice

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Settings loader
# ---------------------------------------------------------------------------

def _load_settings(cursor) -> dict:
    """Load all settings rows into a key→value dict."""
    cursor.execute("SELECT key_name, key_value FROM settings")
    return {row["key_name"]: row["key_value"] for row in cursor.fetchall()}


# ---------------------------------------------------------------------------
# Item master lookup
# ---------------------------------------------------------------------------

def _load_item_master(cursor) -> dict:
    """
    Returns dict: item_code → {earns_points, points_rate}
    Only loads active items.
    """
    cursor.execute(
        "SELECT item_code, earns_points, points_rate FROM item_master WHERE is_active = 1"
    )
    return {
        row["item_code"]: {
            "earns_points": row["earns_points"],
            "points_rate":  float(row["points_rate"]),
        }
        for row in cursor.fetchall()
    }


# ---------------------------------------------------------------------------
# Points calculation
# ---------------------------------------------------------------------------

def _calculate_points(eligible_amount: float, points_rate: float) -> float:
    """
    Formula: FLOOR(eligible_amount / 100) × points_rate
    e.g. ₹850 eligible at rate 1.0 → FLOOR(8.5) × 1.0 = 8 points
    """
    return math.floor(eligible_amount / 100) * points_rate


# ---------------------------------------------------------------------------
# Tier calculator
# ---------------------------------------------------------------------------

def _calculate_tier(total_earned: float, settings: dict) -> str:
    platinum_min = float(settings.get("tier_platinum_min", 10000))
    gold_min     = float(settings.get("tier_gold_min",     2500))
    if total_earned >= platinum_min:
        return "platinum"
    elif total_earned >= gold_min:
        return "gold"
    return "silver"


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

def process_invoices(
    invoices:       list[ParsedInvoice],
    batch_id:       int,
    db_conn,
) -> dict:
    """
    Insert invoices, invoice_lines, and points_log entries.
    Updates contractor balances and tiers.
    Returns counters dict.
    """
    cursor = db_conn.cursor(dictionary=True)

    settings    = _load_settings(cursor)
    item_master = _load_item_master(cursor)
    expiry_days = int(settings.get("points_expiry_days", 365))

    counters = dict(
        invoices_imported  = 0,
        invoices_skipped   = 0,
        invoices_duplicate = 0,
        points_awarded     = 0.0,
        errors             = 0,
    )
    error_notes: list[str] = []

    # Track which contractor balances need updating after all inserts
    contractor_ids_to_update: set[int] = set()

    for inv in invoices:
        try:
            _process_single_invoice(
                inv                       = inv,
                batch_id                  = batch_id,
                cursor                    = cursor,
                item_master               = item_master,
                expiry_days               = expiry_days,
                counters                  = counters,
                contractor_ids_to_update  = contractor_ids_to_update,
                error_notes               = error_notes,
            )
        except Exception as exc:
            log.error("Unexpected error processing invoice %s: %s", inv.bill_number, exc)
            counters["errors"] += 1
            error_notes.append(f"{inv.bill_number}: {exc}")

    # Recompute balances and tiers for all affected contractors
    for cid in contractor_ids_to_update:
        _update_contractor_balance(cursor, cid, settings)

    db_conn.commit()
    cursor.close()

    if error_notes:
        counters["notes"] = "; ".join(error_notes[:10])  # cap at 10 in notes field
    else:
        counters["notes"] = None

    return counters


def _process_single_invoice(
    inv:                      ParsedInvoice,
    batch_id:                 int,
    cursor,
    item_master:              dict,
    expiry_days:              int,
    counters:                 dict,
    contractor_ids_to_update: set,
    error_notes:              list,
) -> None:
    """Process and insert one invoice + its lines + points_log entry."""

    # --- Skip internal invoices ---
    if inv.invoice_type == "internal":
        counters["invoices_skipped"] += 1
        return

    # --- Check for duplicate bill number ---
    cursor.execute(
        "SELECT id FROM invoices WHERE bill_number = %s", (inv.bill_number,)
    )
    if cursor.fetchone():
        counters["invoices_duplicate"] += 1
        counters["invoices_skipped"] += 1
        return

    # --- Calculate eligible amount and points ---
    eligible_amount  = 0.0
    total_points     = 0.0
    line_data        = []   # list of tuples for bulk insert

    for line in inv.lines:
        item_info    = item_master.get(line.item_code)
        earns_points = item_info["earns_points"] if item_info else 0
        points_rate  = item_info["points_rate"]  if item_info else 0.0
        line_eligible = line.line_amount if earns_points else 0.0

        if earns_points and inv.contractor_id:
            line_points = _calculate_points(line.line_amount, points_rate)
            total_points += line_points

        eligible_amount += line_eligible
        line_data.append((
            line.item_code,
            line.item_name,
            line.quantity,
            line.unit,
            line.unit_price,
            line.line_amount,
            earns_points,
            points_rate,
            line_eligible,
        ))

    # --- Determine points_status ---
    if inv.invoice_type == "internal":
        points_status = "not_applicable"
    elif inv.contractor_id is None:
        points_status = "not_applicable"
    else:
        # Check contractor approval status
        cursor.execute(
            "SELECT status FROM contractors WHERE id = %s", (inv.contractor_id,)
        )
        contractor_row = cursor.fetchone()
        if not contractor_row:
            points_status = "not_applicable"
            inv.contractor_id = None
        elif contractor_row["status"] != "approved":
            points_status = "pending"
            total_points  = 0.0   # freeze — no points for unapproved
        elif eligible_amount == 0 or total_points == 0:
            points_status = "skipped"
            total_points  = 0.0
        else:
            points_status = "credited"

    # --- Insert invoice ---
    cursor.execute("""
        INSERT INTO invoices (
            import_batch_id, invoice_date, bill_number, particulars,
            party_name, party_mobile, referred_by_raw,
            invoice_type, contractor_id,
            gross_amount, eligible_amount,
            points_awarded, points_status,
            points_credited_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s
        )
    """, (
        batch_id,
        inv.invoice_date,
        inv.bill_number,
        inv.particulars,
        inv.party_name,
        inv.party_mobile or None,
        inv.referred_by_raw or None,
        inv.invoice_type,
        inv.contractor_id,
        round(inv.gross_amount, 2),
        round(eligible_amount, 2),
        round(total_points, 2),
        points_status,
        datetime.utcnow() if points_status == "credited" else None,
    ))
    invoice_id = cursor.lastrowid

    # --- Insert invoice lines ---
    if line_data:
        cursor.executemany("""
            INSERT INTO invoice_lines (
                invoice_id, item_code, item_name,
                quantity, unit, unit_price, line_amount,
                earns_points, points_rate, eligible_amount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [(invoice_id, *row) for row in line_data])

    # --- Insert points_log entry if points credited ---
    if points_status == "credited" and total_points > 0 and inv.contractor_id:
        expires_at = datetime.utcnow() + timedelta(days=expiry_days)
        cursor.execute("""
            INSERT INTO points_log (
                contractor_id, invoice_id, event_type,
                points, eligible_amount, expires_at, is_expired
            ) VALUES (%s, %s, 'earned', %s, %s, %s, 0)
        """, (
            inv.contractor_id,
            invoice_id,
            round(total_points, 2),
            round(eligible_amount, 2),
            expires_at,
        ))
        counters["points_awarded"] += total_points
        contractor_ids_to_update.add(inv.contractor_id)

    counters["invoices_imported"] += 1


# ---------------------------------------------------------------------------
# Contractor balance recompute
# ---------------------------------------------------------------------------

def _update_contractor_balance(cursor, contractor_id: int, settings: dict) -> None:
    """
    Recomputes points_balance and tier for one contractor from points_log.
    Called after all invoices in a batch are processed.
    """
    cursor.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN event_type = 'earned'   THEN points ELSE 0 END), 0) AS total_earned,
            COALESCE(SUM(CASE WHEN event_type = 'redeemed' THEN ABS(points) ELSE 0 END), 0) AS total_redeemed,
            COALESCE(SUM(CASE WHEN event_type = 'expired'  THEN ABS(points) ELSE 0 END), 0) AS total_expired
        FROM points_log
        WHERE contractor_id = %s
    """, (contractor_id,))
    row = cursor.fetchone()

    total_earned   = float(row["total_earned"])
    total_redeemed = float(row["total_redeemed"])
    total_expired  = float(row["total_expired"])
    balance        = total_earned - total_redeemed - total_expired
    tier           = _calculate_tier(total_earned, settings)

    cursor.execute("""
        UPDATE contractors
        SET
            total_points_earned   = %s,
            total_points_redeemed = %s,
            total_points_expired  = %s,
            points_balance        = %s,
            tier                  = %s
        WHERE id = %s
    """, (
        round(total_earned,   2),
        round(total_redeemed, 2),
        round(total_expired,  2),
        round(balance,        2),
        tier,
        contractor_id,
    ))