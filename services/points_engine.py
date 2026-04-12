"""
services/points_engine.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Points engine — uses invoice_type and customer_type separately:
  invoice_type = sale        → award points if contractor and eligible items
  invoice_type = sale_return → deduct points via 'reversed' log entry
  invoice_type = internal    → import invoice, no points
  customer_type = walk_in    → import invoice, no points unless contractor matched
"""

import logging
import math
from datetime import datetime, timedelta

from services.import_service import (
    ParsedInvoice,
    INV_SALE, INV_SALE_RETURN, INV_INTERNAL,
    CUST_CONTRACTOR_DIRECT, CUST_CONTRACTOR_REFERRED,
)

log = logging.getLogger(__name__)


def _load_settings(cursor) -> dict:
    cursor.execute("SELECT key_name, key_value FROM settings")
    return {row["key_name"]: row["key_value"] for row in cursor.fetchall()}


def _load_item_master(cursor) -> dict:
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


def _calculate_points(amount: float, points_rate: float) -> float:
    """FLOOR(abs(amount) / 100) × points_rate"""
    return math.floor(abs(amount) / 100) * points_rate


def _calculate_tier(total_earned: float, settings: dict) -> str:
    if total_earned >= float(settings.get("tier_platinum_min", 10000)):
        return "platinum"
    elif total_earned >= float(settings.get("tier_gold_min", 2500)):
        return "gold"
    return "silver"


def process_invoices(invoices: list[ParsedInvoice], batch_id: int, db_conn) -> dict:
    cursor = db_conn.cursor(dictionary=True)
    settings    = _load_settings(cursor)
    item_master = _load_item_master(cursor)
    expiry_days = int(settings.get("points_expiry_days", 365))

    counters = dict(
        invoices_imported=0, invoices_skipped=0,
        invoices_duplicate=0, points_awarded=0.0, errors=0,
    )
    error_notes: list[str] = []
    contractor_ids_to_update: set[int] = set()

    for inv in invoices:
        try:
            _process_single(inv, batch_id, cursor, item_master, expiry_days,
                            counters, contractor_ids_to_update, error_notes)
        except Exception as exc:
            log.error("Error on invoice %s: %s", inv.bill_number, exc)
            counters["errors"] += 1
            error_notes.append(f"{inv.bill_number}: {exc}")

    for cid in contractor_ids_to_update:
        _update_contractor_balance(cursor, cid, settings)

    db_conn.commit()
    cursor.close()
    counters["notes"] = "; ".join(error_notes[:10]) if error_notes else None
    return counters


def _process_single(inv, batch_id, cursor, item_master, expiry_days,
                    counters, contractor_ids_to_update, error_notes):

    # Duplicate check
    cursor.execute("SELECT id FROM invoices WHERE bill_number = %s", (inv.bill_number,))
    if cursor.fetchone():
        counters["invoices_duplicate"] += 1
        counters["invoices_skipped"] += 1
        return

    # Calculate eligible amount and points per line
    eligible_amount = 0.0
    total_points    = 0.0
    line_data       = []

    for line in inv.lines:
        item_info    = item_master.get(line.item_code)
        earns_points = item_info["earns_points"] if item_info else 0
        points_rate  = item_info["points_rate"]  if item_info else 0.0
        line_eligible = abs(line.line_amount) if earns_points else 0.0
        eligible_amount += line_eligible

        if earns_points and inv.contractor_id:
            total_points += _calculate_points(line.line_amount, points_rate)

        line_data.append((
            line.item_code, line.item_name,
            line.quantity, line.unit,
            line.unit_price, line.line_amount,
            earns_points, points_rate, line_eligible,
        ))

    # Determine points_status
    points_status = "not_applicable"
    contractor_status = None

    if inv.contractor_id:
        cursor.execute("SELECT status FROM contractors WHERE id = %s", (inv.contractor_id,))
        row = cursor.fetchone()
        contractor_status = row["status"] if row else None

    if inv.invoice_type == INV_INTERNAL:
        points_status = "not_applicable"
        total_points  = 0.0

    elif not inv.contractor_id:
        points_status = "not_applicable"
        total_points  = 0.0

    elif contractor_status != "approved":
        points_status = "pending"
        total_points  = 0.0

    elif eligible_amount == 0 or total_points == 0:
        points_status = "skipped"
        total_points  = 0.0

    else:
        points_status = "credited"

    # Insert invoice
    cursor.execute("""
        INSERT INTO invoices (
            import_batch_id, invoice_date, bill_number, particulars,
            party_name, party_mobile, referred_by_raw,
            invoice_type, contractor_id,
            gross_amount, eligible_amount,
            points_awarded, points_status, points_credited_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        batch_id, inv.invoice_date, inv.bill_number, inv.particulars,
        inv.party_name, inv.party_mobile or None, inv.referred_by_raw or None,
        inv.invoice_type, inv.contractor_id,
        round(inv.gross_amount, 2), round(eligible_amount, 2),
        round(total_points, 2), points_status,
        datetime.utcnow() if points_status == "credited" else None,
    ))
    invoice_id = cursor.lastrowid

    # Also store customer_type — UPDATE after insert
    cursor.execute(
        "UPDATE invoices SET customer_type = %s WHERE id = %s",
        (inv.customer_type, invoice_id)
    )

    # Insert lines
    if line_data:
        cursor.executemany("""
            INSERT INTO invoice_lines (
                invoice_id, item_code, item_name,
                quantity, unit, unit_price, line_amount,
                earns_points, points_rate, eligible_amount
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [(invoice_id, *row) for row in line_data])

    # Insert points_log entry
    if points_status == "credited" and total_points > 0 and inv.contractor_id:
        if inv.invoice_type == INV_SALE_RETURN:
            # Deduct points
            cursor.execute("""
                INSERT INTO points_log (
                    contractor_id, invoice_id, event_type,
                    points, eligible_amount, notes
                ) VALUES (%s, %s, 'reversed', %s, %s, %s)
            """, (
                inv.contractor_id, invoice_id,
                -round(total_points, 2),
                round(eligible_amount, 2),
                f"Sale return: {inv.bill_number}",
            ))
            counters["points_awarded"] -= total_points
        else:
            # Award points
            expires_at = datetime.utcnow() + timedelta(days=expiry_days)
            cursor.execute("""
                INSERT INTO points_log (
                    contractor_id, invoice_id, event_type,
                    points, eligible_amount, expires_at, is_expired
                ) VALUES (%s, %s, 'earned', %s, %s, %s, 0)
            """, (
                inv.contractor_id, invoice_id,
                round(total_points, 2),
                round(eligible_amount, 2),
                expires_at,
            ))
            counters["points_awarded"] += total_points

        contractor_ids_to_update.add(inv.contractor_id)

    counters["invoices_imported"] += 1


def _update_contractor_balance(cursor, contractor_id: int, settings: dict) -> None:
    cursor.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN event_type = 'earned'                 THEN points       ELSE 0 END), 0) AS total_earned,
            COALESCE(SUM(CASE WHEN event_type = 'redeemed'               THEN ABS(points)  ELSE 0 END), 0) AS total_redeemed,
            COALESCE(SUM(CASE WHEN event_type = 'expired'                THEN ABS(points)  ELSE 0 END), 0) AS total_expired,
            COALESCE(SUM(CASE WHEN event_type IN ('reversed','adjusted') THEN points       ELSE 0 END), 0) AS total_adjustments
        FROM points_log WHERE contractor_id = %s
    """, (contractor_id,))
    row = cursor.fetchone()

    total_earned      = float(row["total_earned"])
    total_redeemed    = float(row["total_redeemed"])
    total_expired     = float(row["total_expired"])
    total_adjustments = float(row["total_adjustments"])
    balance           = total_earned - total_redeemed - total_expired + total_adjustments

    cursor.execute("""
        UPDATE contractors SET
            total_points_earned   = %s,
            total_points_redeemed = %s,
            total_points_expired  = %s,
            points_balance        = %s,
            tier                  = %s
        WHERE id = %s
    """, (
        round(total_earned, 2), round(total_redeemed, 2),
        round(total_expired, 2), round(balance, 2),
        _calculate_tier(total_earned, settings),
        contractor_id,
    ))