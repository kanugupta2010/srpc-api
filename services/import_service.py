"""
services/import_service.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Invoice parser — reads Busy 21 CSV export and resolves contractor attribution.

Busy 21 export format:
  - Multi-row invoices: header fields (Date, Bill No, Particulars, Party Name,
    Party Mobile, Referred By) appear only on the FIRST row of each invoice.
  - Subsequent rows for the same invoice have only line item fields:
    Alias, Item Details, Qty, Unit, Price, Amount.
  - The script carries header fields forward until a new bill number appears.
"""

import csv
import io
import re
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column name constants — exact headers from Busy 21 export
# ---------------------------------------------------------------------------
COL_DATE         = "Date"
COL_BILL_NO      = "Vch/Bill No"
COL_PARTICULARS  = "Particulars"
COL_ALIAS        = "Alias"
COL_ITEM_DETAILS = "Item Details"
COL_QTY          = "Qty"
COL_UNIT         = "Unit"
COL_PRICE        = "Price"
COL_AMOUNT       = "Amount"
COL_PARTY_NAME   = "Party Name"
COL_PARTY_MOBILE = "Party Mobile"
COL_REFERRED_BY  = "Referred By"

REQUIRED_COLS = {COL_BILL_NO, COL_ALIAS, COL_AMOUNT}

# Pattern to extract mobile from "Name - Mobile" format
REFERRED_BY_RE = re.compile(r"-\s*(\d{10,12})\s*$")

# Self consumption marker
SELF_CONSUMPTION = "self consumption of goods"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ParsedLine:
    item_code:   str
    item_name:   str
    quantity:    float
    unit:        str
    unit_price:  float
    line_amount: float


@dataclass
class ParsedInvoice:
    invoice_date:     Optional[date]
    bill_number:      str
    particulars:      str
    party_name:       str
    party_mobile:     str
    referred_by_raw:  str
    referred_by_mobile: Optional[str]   # extracted from referred_by_raw
    invoice_type:     str               # contractor_direct | contractor_referred | walk_in | internal
    contractor_id:    Optional[int]     # resolved after DB lookup
    lines:            list = field(default_factory=list)

    @property
    def gross_amount(self) -> float:
        return sum(l.line_amount for l in self.lines)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """Parse Busy 21 date — tries common Indian formats."""
    v = raw.strip()
    if not v:
        return None
    from datetime import datetime
    formats = ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y",
               "%d %b %Y", "%d-%m-%y", "%d/%m/%y")
    for fmt in formats:
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    log.warning("Could not parse invoice date '%s'", raw)
    return None


def _parse_float(raw: str) -> float:
    v = raw.strip().replace(",", "")
    if not v:
        return 0.0
    try:
        return float(v)
    except ValueError:
        return 0.0


def _extract_mobile_from_referred_by(raw: str) -> Optional[str]:
    """
    Extract mobile number from 'Name - Mobile' format.
    Returns 10-12 digit string or None.
    """
    if not raw:
        return None
    m = REFERRED_BY_RE.search(raw.strip())
    return m.group(1) if m else None


def _clean_mobile(raw: str) -> str:
    """Strip spaces, leading +91, leading 0."""
    v = raw.strip().replace(" ", "").replace("-", "")
    if v.startswith("+91"):
        v = v[3:]
    elif v.startswith("91") and len(v) == 12:
        v = v[2:]
    if v.startswith("0") and len(v) == 11:
        v = v[1:]
    return v


# ---------------------------------------------------------------------------
# CSV parser
# ---------------------------------------------------------------------------

def parse_csv(content: bytes) -> tuple[list[ParsedInvoice], dict]:
    """
    Parse Busy 21 CSV export bytes into a list of ParsedInvoice objects.
    Returns (invoices, stats) where stats has raw row counts.
    """
    text = content.decode("utf-8-sig")  # strip BOM
    reader = csv.DictReader(io.StringIO(text))
    reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]

    missing = REQUIRED_COLS - set(reader.fieldnames)
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {sorted(missing)}. "
            f"Found: {reader.fieldnames}"
        )

    invoices: list[ParsedInvoice] = []
    current: Optional[ParsedInvoice] = None

    stats = {"total_rows": 0, "blank_rows": 0, "line_rows": 0, "header_rows": 0}

    # Carry-forward header fields (only on first row of each invoice)
    for row in reader:
        row = {k.strip(): (v.strip() if v else "") for k, v in row.items()}
        stats["total_rows"] += 1

        bill_no   = row.get(COL_BILL_NO, "").strip()
        alias     = row.get(COL_ALIAS, "").strip()
        amount_raw = row.get(COL_AMOUNT, "").strip()

        # Completely blank row — skip
        if not bill_no and not alias and not amount_raw:
            stats["blank_rows"] += 1
            continue

        # New invoice header row
        if bill_no:
            stats["header_rows"] += 1

            particulars = row.get(COL_PARTICULARS, "").strip()

            # Detect self consumption — skip this invoice entirely
            if SELF_CONSUMPTION in particulars.lower():
                current = None
                continue

            referred_by_raw    = row.get(COL_REFERRED_BY, "").strip()
            referred_by_mobile = _extract_mobile_from_referred_by(referred_by_raw)
            if referred_by_mobile:
                referred_by_mobile = _clean_mobile(referred_by_mobile)

            party_mobile_raw = row.get(COL_PARTY_MOBILE, "").strip()
            party_mobile     = _clean_mobile(party_mobile_raw) if party_mobile_raw else ""

            # Determine provisional invoice type (contractor_id resolved later)
            if referred_by_mobile:
                inv_type = "contractor_referred"
            elif particulars.lower() not in ("cash", ""):
                inv_type = "contractor_direct"   # may be confirmed after DB lookup
            else:
                inv_type = "walk_in"

            current = ParsedInvoice(
                invoice_date       = _parse_date(row.get(COL_DATE, "")),
                bill_number        = bill_no,
                particulars        = particulars,
                party_name         = row.get(COL_PARTY_NAME, "").strip(),
                party_mobile       = party_mobile,
                referred_by_raw    = referred_by_raw,
                referred_by_mobile = referred_by_mobile,
                invoice_type       = inv_type,
                contractor_id      = None,
            )
            invoices.append(current)

        # Line item row (no bill number — belongs to current invoice)
        if alias and current is not None:
            stats["line_rows"] += 1
            line = ParsedLine(
                item_code   = alias[:100],
                item_name   = row.get(COL_ITEM_DETAILS, "").strip()[:255],
                quantity    = _parse_float(row.get(COL_QTY, "")),
                unit        = row.get(COL_UNIT, "").strip()[:20],
                unit_price  = _parse_float(row.get(COL_PRICE, "")),
                line_amount = _parse_float(row.get(COL_AMOUNT, "")),
            )
            current.lines.append(line)

    return invoices, stats


# ---------------------------------------------------------------------------
# Contractor resolver
# ---------------------------------------------------------------------------

def resolve_contractors(invoices: list[ParsedInvoice], db_conn) -> None:
    """
    Resolves contractor_id for each invoice by querying the DB.
    Mutates invoices in place.

    Logic:
    1. contractor_referred  → match referred_by_mobile against contractors.mobile
    2. contractor_direct    → match particulars against contractor_code first,
                              then fall back to party_mobile
    3. walk_in              → try party_mobile against contractors.mobile
    """
    cursor = db_conn.cursor(dictionary=True)

    # Build lookup maps from DB — mobile → contractor row
    cursor.execute(
        "SELECT id, contractor_code, mobile, status FROM contractors WHERE is_active = 1"
    )
    rows = cursor.fetchall()
    mobile_map: dict[str, dict] = {}
    code_map:   dict[str, dict] = {}
    for r in rows:
        if r["mobile"]:
            mobile_map[r["mobile"].strip()] = r
        if r["contractor_code"]:
            code_map[r["contractor_code"].strip()] = r

    cursor.close()

    for inv in invoices:
        contractor = None

        if inv.invoice_type == "contractor_referred":
            contractor = mobile_map.get(inv.referred_by_mobile or "")

        elif inv.invoice_type == "contractor_direct":
            # Try contractor_code match first
            contractor = code_map.get(inv.particulars)
            # Fall back to party_mobile
            if not contractor and inv.party_mobile:
                contractor = mobile_map.get(inv.party_mobile)
            # If still not found, downgrade to walk_in
            if not contractor:
                inv.invoice_type = "walk_in"

        elif inv.invoice_type == "walk_in":
            if inv.party_mobile:
                contractor = mobile_map.get(inv.party_mobile)
                if contractor:
                    inv.invoice_type = "contractor_direct"

        if contractor:
            inv.contractor_id = contractor["id"]
            # If contractor is not approved, freeze points
            if contractor["status"] != "approved":
                inv.invoice_type = inv.invoice_type  # keep type
                inv.contractor_id = contractor["id"]  # still link them