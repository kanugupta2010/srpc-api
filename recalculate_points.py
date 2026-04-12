"""
recalculate_points.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Recalculates all points from scratch based on existing invoices and
current item_master settings.

When to run this:
  - After changing earns_points or points_rate in item_master
  - After approving contractors who had pending invoices
  - After correcting contractor attribution on invoices
  - Any time points balances seem incorrect

What it does:
  1. Clears all points_log entries of type 'earned' and 'reversed'
     (manual 'adjusted' entries are preserved)
  2. Recalculates points for every credited/pending invoice
  3. Rewrites points_log entries
  4. Recomputes all contractor balances and tiers

Usage:
    python recalculate_points.py

    # Dry run — shows what would change without writing to DB:
    python recalculate_points.py --dry-run
"""

import os
import sys
import math
import logging
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error as MySQLError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

load_dotenv()

DB_HOST     = os.getenv("DB_HOST")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT     = int(os.getenv("DB_PORT", 3306))


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def get_conn():
    try:
        return mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            charset="utf8mb4", collation="utf8mb4_unicode_ci",
            connection_timeout=10,
        )
    except MySQLError as exc:
        log.error("DB connection failed: %s", exc)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_settings(cursor) -> dict:
    cursor.execute("SELECT key_name, key_value FROM settings")
    return {r["key_name"]: r["key_value"] for r in cursor.fetchall()}


def load_item_master(cursor) -> dict:
    cursor.execute(
        "SELECT item_code, earns_points, points_rate FROM item_master WHERE is_active = 1"
    )
    return {
        r["item_code"]: {
            "earns_points": r["earns_points"],
            "points_rate":  float(r["points_rate"]),
        }
        for r in cursor.fetchall()
    }


def load_contractors(cursor) -> dict:
    """Returns dict: contractor_id → status"""
    cursor.execute("SELECT id, status FROM contractors WHERE is_active = 1")
    return {r["id"]: r["status"] for r in cursor.fetchall()}


# ---------------------------------------------------------------------------
# Points formula
# ---------------------------------------------------------------------------

def calculate_points(amount: float, points_rate: float) -> float:
    return math.floor(abs(amount) / 100) * points_rate


def calculate_tier(total_earned: float, settings: dict) -> str:
    if total_earned >= float(settings.get("tier_platinum_min", 10000)):
        return "platinum"
    elif total_earned >= float(settings.get("tier_gold_min", 2500)):
        return "gold"
    return "silver"


# ---------------------------------------------------------------------------
# Core recalculation
# ---------------------------------------------------------------------------

def recalculate(dry_run: bool = False) -> None:
    conn   = get_conn()
    cursor = conn.cursor(dictionary=True)

    settings    = load_settings(cursor)
    item_master = load_item_master(cursor)
    contractors = load_contractors(cursor)
    expiry_days = int(settings.get("points_expiry_days", 365))

    log.info("=" * 60)
    log.info("SRPC Points Recalculation — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("Dry run: %s", dry_run)
    log.info("=" * 60)

    # --- Step 1: Load all invoices that could have points ---
    cursor.execute("""
        SELECT
            i.id, i.bill_number, i.invoice_date, i.invoice_type,
            i.contractor_id, i.points_credited_at
        FROM invoices i
        WHERE i.contractor_id IS NOT NULL
        ORDER BY i.invoice_date ASC, i.id ASC
    """)
    invoices = cursor.fetchall()
    log.info("Invoices with contractor attribution: %d", len(invoices))

    # --- Step 2: Load all invoice lines ---
    cursor.execute("""
        SELECT il.invoice_id, il.item_code, il.line_amount
        FROM invoice_lines il
        JOIN invoices i ON i.id = il.invoice_id
        WHERE i.contractor_id IS NOT NULL
    """)
    lines_raw = cursor.fetchall()

    # Group lines by invoice_id
    lines_by_invoice: dict[int, list] = {}
    for line in lines_raw:
        lines_by_invoice.setdefault(line["invoice_id"], []).append(line)

    # --- Step 3: Clear existing earned/reversed log entries ---
    cursor.execute("""
        SELECT COUNT(*) AS cnt FROM points_log
        WHERE event_type IN ('earned', 'reversed')
    """)
    existing_count = cursor.fetchone()["cnt"]
    log.info("Existing earned/reversed entries to clear: %d", existing_count)

    if not dry_run:
        cursor.execute(
            "DELETE FROM points_log WHERE event_type IN ('earned', 'reversed')"
        )
        log.info("Cleared %d points_log entries", existing_count)

    # --- Step 4: Recalculate and reinsert ---
    new_entries   = 0
    total_points  = 0.0
    invoice_updates: list[tuple] = []   # (points_awarded, points_status, invoice_id)
    contractor_ids_affected: set[int] = set()

    for inv in invoices:
        inv_id         = inv["id"]
        contractor_id  = inv["contractor_id"]
        invoice_type   = inv["invoice_type"]
        invoice_date   = inv["invoice_date"]
        contractor_status = contractors.get(contractor_id)

        lines = lines_by_invoice.get(inv_id, [])

        # Calculate eligible amount and points per line using CURRENT item_master
        eligible_amount = 0.0
        inv_points      = 0.0

        for line in lines:
            item_info    = item_master.get(line["item_code"])
            earns_points = item_info["earns_points"] if item_info else 0
            points_rate  = item_info["points_rate"]  if item_info else 0.0
            if earns_points:
                eligible_amount += abs(line["line_amount"])
                inv_points      += calculate_points(line["line_amount"], points_rate)

        # Determine points_status
        if contractor_status != "approved":
            points_status = "pending"
            inv_points    = 0.0
        elif eligible_amount == 0 or inv_points == 0:
            points_status = "skipped"
            inv_points    = 0.0
        else:
            points_status = "credited"

        invoice_updates.append((
            round(eligible_amount, 2),
            round(inv_points, 2),
            points_status,
            inv_id,
        ))

        if points_status == "credited" and inv_points > 0:
            # Calculate expires_at from invoice_date
            if invoice_date:
                expires_at = datetime.combine(invoice_date, datetime.min.time()) + timedelta(days=expiry_days)
            else:
                expires_at = datetime.utcnow() + timedelta(days=expiry_days)

            event_type = "reversed" if invoice_type == "sale_return" else "earned"
            signed_pts = -round(inv_points, 2) if invoice_type == "sale_return" else round(inv_points, 2)

            if not dry_run:
                if invoice_type == "sale_return":
                    cursor.execute("""
                        INSERT INTO points_log (
                            contractor_id, invoice_id, event_type,
                            points, eligible_amount, notes
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        contractor_id, inv_id, event_type,
                        signed_pts, round(eligible_amount, 2),
                        f"Recalculated — sale return: {inv['bill_number']}",
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO points_log (
                            contractor_id, invoice_id, event_type,
                            points, eligible_amount, expires_at, is_expired
                        ) VALUES (%s, %s, %s, %s, %s, %s, 0)
                    """, (
                        contractor_id, inv_id, event_type,
                        signed_pts, round(eligible_amount, 2), expires_at,
                    ))

            new_entries  += 1
            total_points += abs(inv_points)
            contractor_ids_affected.add(contractor_id)

            log.info(
                "  %s %s | contractor %d | eligible ₹%.2f | points %.2f | %s",
                event_type.upper(), inv["bill_number"],
                contractor_id, eligible_amount, inv_points, points_status,
            )

    # --- Step 5: Update invoice eligible_amount, points_awarded, points_status ---
    if not dry_run:
        cursor.executemany("""
            UPDATE invoices
            SET eligible_amount = %s, points_awarded = %s, points_status = %s
            WHERE id = %s
        """, invoice_updates)
        log.info("Updated %d invoice records", len(invoice_updates))

    # --- Step 6: Recompute contractor balances ---
    log.info("Recomputing balances for %d contractors...", len(contractor_ids_affected))

    for cid in contractor_ids_affected:
        if dry_run:
            log.info("  [DRY RUN] Would update balance for contractor %d", cid)
            continue

        cursor.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN event_type = 'earned'                 THEN points      ELSE 0 END), 0) AS total_earned,
                COALESCE(SUM(CASE WHEN event_type = 'redeemed'               THEN ABS(points) ELSE 0 END), 0) AS total_redeemed,
                COALESCE(SUM(CASE WHEN event_type = 'expired'                THEN ABS(points) ELSE 0 END), 0) AS total_expired,
                COALESCE(SUM(CASE WHEN event_type IN ('reversed','adjusted') THEN points      ELSE 0 END), 0) AS total_adjustments
            FROM points_log WHERE contractor_id = %s
        """, (cid,))
        row = cursor.fetchone()

        total_earned      = float(row["total_earned"])
        total_redeemed    = float(row["total_redeemed"])
        total_expired     = float(row["total_expired"])
        total_adjustments = float(row["total_adjustments"])
        balance           = total_earned - total_redeemed - total_expired + total_adjustments
        tier              = calculate_tier(total_earned, settings)

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
            tier, cid,
        ))
        log.info(
            "  Contractor %d — earned: %.2f | balance: %.2f | tier: %s",
            cid, total_earned, balance, tier,
        )

    if not dry_run:
        conn.commit()

    cursor.close()
    conn.close()

    log.info("-" * 60)
    log.info("Recalculation complete:")
    log.info("  Invoices processed      : %d", len(invoices))
    log.info("  Points log entries written : %d", new_entries)
    log.info("  Total points awarded    : %.2f", total_points)
    log.info("  Contractors updated     : %d", len(contractor_ids_affected))
    if dry_run:
        log.info("  *** DRY RUN — no changes written to DB ***")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recalculate all loyalty points from invoices.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing to the database.",
    )
    args = parser.parse_args()
    recalculate(dry_run=args.dry_run)