"""
sync_item_master.py
SRPC Enterprises Private Limited — Saraswati Hardware, Paints & Sanitary
Loyalty Program — Item Master Sync

Pulls the item master Google Sheet (published as CSV) and UPSERTs
into the item_master table in MySQL.

Usage:
    python sync_item_master.py

Requirements:
    pip install requests mysql-connector-python python-dotenv

.env file keys required:
    SHEET_ID       — Google Sheet ID
    DB_HOST        — MySQL host (AWS RDS endpoint)
    DB_NAME        — Database name (contractorconnect)
    DB_USER        — MySQL username
    DB_PASSWORD    — MySQL password
    DB_PORT        — MySQL port (optional, defaults to 3306)
    SHEET_NAME     — Sheet tab name (optional, defaults to Sheet1)
"""

import os
import sys
import csv
import io
import logging
from datetime import datetime

import requests
import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

SHEET_ID    = os.getenv("SHEET_ID")
SHEET_NAME  = os.getenv("SHEET_NAME", "Sheet1")
DB_HOST     = os.getenv("DB_HOST")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT     = int(os.getenv("DB_PORT", 3306))

# ---------------------------------------------------------------------------
# Column name constants — exact headers from the Google Sheet
# ---------------------------------------------------------------------------
COL_DATE_OP1            = "Date - Op 1"
COL_ITEM_NAME           = "Item_Name"
COL_PRINT_NAME          = "Print Name"
COL_ALIAS               = "Alias"
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

# Columns ignored entirely — not stored in DB
SKIP_COLS = {
    "Item Name Length",
    "Print Name Length",
    "Rates till 29 Mar",
    "P Key - Op2",
}

# Minimum required columns to proceed
REQUIRED_COLS = {COL_ALIAS, COL_ITEM_NAME, COL_EARNS_POINTS, COL_POINTS_RATE}

# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_earns_points(raw: str) -> int:
    """Accept 1/0, yes/no, true/false — return 0 or 1."""
    v = raw.strip().lower()
    if v in ("1", "yes", "true"):
        return 1
    return 0


def parse_points_rate(raw: str) -> float:
    """Parse points rate — default 0.0 on blank or invalid."""
    v = raw.strip()
    if not v:
        return 0.0
    try:
        rate = float(v)
        if rate < 0:
            raise ValueError("Negative rate not allowed")
        return round(rate, 4)
    except ValueError:
        log.warning("Invalid points_rate value '%s' — defaulting to 0", raw)
        return 0.0


def parse_decimal(raw: str, field_name: str):
    """Parse a price field — return None on blank, warn on invalid."""
    v = raw.strip().replace(",", "")  # handle comma-formatted numbers
    if not v:
        return None
    try:
        return round(float(v), 2)
    except ValueError:
        log.warning("Invalid decimal '%s' in '%s' — storing NULL", raw, field_name)
        return None


def parse_date_ist(raw: str):
    """
    Parse date from sheet Column A (Date - Op 1).
    Date is entered in India (IST) — stored as DATE, no timezone conversion needed.
    Handles common Indian date formats.
    Returns a date object or None.
    """
    v = raw.strip()
    if not v:
        return None
    formats = (
        "%d-%m-%Y",   # 15-03-2024
        "%d/%m/%Y",   # 15/03/2024
        "%Y-%m-%d",   # 2024-03-15
        "%d-%b-%Y",   # 15-Mar-2024
        "%d %b %Y",   # 15 Mar 2024
        "%d-%m-%y",   # 15-03-24
        "%d/%m/%y",   # 15/03/24
    )
    for fmt in formats:
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    log.warning("Could not parse date '%s' in '%s' — storing NULL", raw, COL_DATE_OP1)
    return None


def infer_category(group: str):
    """Use Group column from sheet as category. Returns None if blank."""
    return group.strip()[:100] if group.strip() else None


# ---------------------------------------------------------------------------
# Sheet fetch
# ---------------------------------------------------------------------------

def build_csv_url(sheet_id: str, sheet_name: str) -> str:
    return (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}"
    )


def fetch_sheet_csv(sheet_id: str, sheet_name: str) -> list:
    url = build_csv_url(sheet_id, sheet_name)
    log.info("Fetching sheet: %s", url)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Failed to fetch sheet: %s", exc)
        sys.exit(1)

    text = resp.content.decode("utf-8-sig")  # strip BOM if present
    reader = csv.DictReader(io.StringIO(text))
    reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]

    missing = REQUIRED_COLS - set(reader.fieldnames)
    if missing:
        log.error(
            "Sheet is missing required columns: %s\nFound columns: %s",
            sorted(missing),
            reader.fieldnames,
        )
        sys.exit(1)

    rows = []
    for row in reader:
        rows.append({k.strip(): (v.strip() if v else "") for k, v in row.items()})

    log.info("Sheet rows fetched: %d", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            charset="utf8mb4",
            collation="utf8mb4_unicode_ci",
            connection_timeout=10,
        )
        return conn
    except MySQLError as exc:
        log.error("Database connection failed: %s", exc)
        sys.exit(1)


UPSERT_SQL = """
INSERT INTO item_master (
    item_code,
    item_name,
    item_print_name,
    category,
    hsn_code,
    unit,
    purchase_price_exc_gst,
    purchase_price_inc_gst,
    sale_price_exc_gst,
    sale_price_inc_gst,
    kacha_sale_price,
    bill_landing,
    tax_category,
    earns_points,
    points_rate,
    item_created_date,
    is_active
) VALUES (
    %(item_code)s,
    %(item_name)s,
    %(item_print_name)s,
    %(category)s,
    %(hsn_code)s,
    %(unit)s,
    %(purchase_price_exc_gst)s,
    %(purchase_price_inc_gst)s,
    %(sale_price_exc_gst)s,
    %(sale_price_inc_gst)s,
    %(kacha_sale_price)s,
    %(bill_landing)s,
    %(tax_category)s,
    %(earns_points)s,
    %(points_rate)s,
    %(item_created_date)s,
    1
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
    item_created_date       = VALUES(item_created_date),
    is_active               = 1,
    updated_at              = CASE
        WHEN item_name                              != VALUES(item_name)
          OR earns_points                           != VALUES(earns_points)
          OR points_rate                            != VALUES(points_rate)
          OR COALESCE(unit,              '')        != COALESCE(VALUES(unit),              '')
          OR COALESCE(hsn_code,          '')        != COALESCE(VALUES(hsn_code),          '')
          OR COALESCE(sale_price_inc_gst, 0)        != COALESCE(VALUES(sale_price_inc_gst), 0)
          OR COALESCE(tax_category,      '')        != COALESCE(VALUES(tax_category),      '')
        THEN CURRENT_TIMESTAMP
        ELSE updated_at
    END
"""


# ---------------------------------------------------------------------------
# Core sync
# ---------------------------------------------------------------------------

def sync(rows: list) -> dict:
    counters = dict(inserted=0, updated=0, unchanged=0, skipped=0, errors=0)
    error_details = []

    conn = get_db_connection()
    cursor = conn.cursor()

    for i, row in enumerate(rows, start=2):  # row 1 = header in sheet
        item_code = row.get(COL_ALIAS, "").strip()
        item_name = row.get(COL_ITEM_NAME, "").strip()

        # Skip blank rows (common at end of Google Sheet exports)
        if not item_code and not item_name:
            counters["skipped"] += 1
            continue

        if not item_code:
            log.warning("Row %d — empty Alias, skipping. Item_Name: '%s'", i, item_name)
            counters["skipped"] += 1
            continue

        if not item_name:
            log.warning("Row %d — empty Item_Name for code '%s', skipping.", i, item_code)
            counters["skipped"] += 1
            continue

        earns_points = parse_earns_points(row.get(COL_EARNS_POINTS, "0"))
        points_rate  = parse_points_rate(row.get(COL_POINTS_RATE, "0"))

        # Business rule: earns_points=0 must force points_rate=0
        if earns_points == 0:
            points_rate = 0.0

        params = dict(
            item_code               = item_code[:100],
            item_name               = item_name[:255],
            item_print_name         = (row.get(COL_PRINT_NAME, "").strip() or None),
            category                = infer_category(row.get(COL_GROUP, "")),
            hsn_code                = (row.get(COL_HSN_CODE, "").strip() or None),
            unit                    = (row.get(COL_UNIT, "").strip() or None),
            purchase_price_exc_gst  = parse_decimal(row.get(COL_PURCHASE_EXC_GST, ""), COL_PURCHASE_EXC_GST),
            purchase_price_inc_gst  = parse_decimal(row.get(COL_PURCHASE_INC_GST, ""), COL_PURCHASE_INC_GST),
            sale_price_exc_gst      = parse_decimal(row.get(COL_SALE_EXC_GST, ""),     COL_SALE_EXC_GST),
            sale_price_inc_gst      = parse_decimal(row.get(COL_SALE_INC_GST, ""),     COL_SALE_INC_GST),
            kacha_sale_price        = parse_decimal(row.get(COL_KACHA_SALE, ""),        COL_KACHA_SALE),
            bill_landing            = parse_decimal(row.get(COL_BILL_LANDING, ""),      COL_BILL_LANDING),
            tax_category            = (row.get(COL_TAX_CATEGORY, "").strip() or None),
            earns_points            = earns_points,
            points_rate             = points_rate,
            item_created_date       = parse_date_ist(row.get(COL_DATE_OP1, "")),
        )

        try:
            cursor.execute(UPSERT_SQL, params)
            rc = cursor.rowcount
            # rowcount: 1 = inserted, 2 = updated, 0 = unchanged
            if rc == 1:
                counters["inserted"] += 1
            elif rc == 2:
                counters["updated"] += 1
            else:
                counters["unchanged"] += 1
        except MySQLError as exc:
            log.error("Row %d — DB error for code '%s': %s", i, item_code, exc)
            error_details.append(f"Row {i} ({item_code}): {exc}")
            counters["errors"] += 1

    conn.commit()
    cursor.close()
    conn.close()

    counters["error_details"] = error_details
    return counters


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def validate_env():
    missing = [k for k in ("SHEET_ID", "DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD")
               if not os.getenv(k)]
    if missing:
        log.error("Missing required environment variables: %s", missing)
        sys.exit(1)


def main():
    validate_env()

    log.info("=" * 60)
    log.info("SRPC Item Master Sync — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    rows = fetch_sheet_csv(SHEET_ID, SHEET_NAME)

    if not rows:
        log.warning("Sheet returned 0 rows — nothing to sync.")
        sys.exit(0)

    result = sync(rows)

    log.info("-" * 60)
    log.info("Sync complete:")
    log.info("  Inserted  : %d", result["inserted"])
    log.info("  Updated   : %d", result["updated"])
    log.info("  Unchanged : %d", result["unchanged"])
    log.info("  Skipped   : %d  (blank / incomplete rows)", result["skipped"])
    log.info("  Errors    : %d", result["errors"])

    if result["error_details"]:
        log.warning("Error details:")
        for detail in result["error_details"]:
            log.warning("  %s", detail)

    log.info("=" * 60)
    sys.exit(1 if result["errors"] else 0)


if __name__ == "__main__":
    main()
