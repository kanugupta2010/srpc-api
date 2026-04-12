"""
services/import_service.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Invoice parser — reads Busy 21 CSV or XLSX export and resolves contractor attribution.

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
from datetime import date, datetime
from typing import Optional

import openpyxl

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
    invoice_date:       Optional[date]
    bill_number:        str
    particulars:        str
    party_name:         str
    party_mobile:       str
    referred_by_raw:    str
    referred_by_mobile: Optional[str]  # extracted from referred_by_raw
    invoice_type:       str            # contractor_direct | contractor_referred | walk_in | internal
    contractor_id:      Optional[int]  # resolved after DB lookup
    lines:              list = field(default_factory=list)

    @property
    def gross_amount(self) -> float:
        return sum(l.line_amount for l in self.lines)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_date(raw) -> Optional[date]:
    """Parse Busy 21 date — handles string and Excel date objects."""
    if raw is None:
        return None
    # openpyxl may return a datetime object directly
    if isinstance(raw, (datetime, date)):
        return raw if isinstance(raw, date) else raw.date()
    v = str(raw).strip()
    if not v:
        return None
    formats = (
        "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d",
        "%d-%b-%Y", "%d %b %Y", "%d-%m-%y", "%d/%m/%y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    log.warning("Could not parse invoice date '%s'", raw)
    return None


def _parse_float(raw) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    v = str(raw).strip().replace(",", "")
    if not v:
        return 0.0
    try:
        return float(v)
    except ValueError:
        return 0.0


def _to_str(raw) -> str:
    """Convert any cell value to a stripped string."""
    if raw is None:
        return ""
    return str(raw).strip()


def _extract_mobile_from_referred_by(raw: str) -> Optional[str]:
    """Extract mobile number from 'Name - Mobile' format."""
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
# Unified row iterator — yields dict rows from CSV or XLSX
# ---------------------------------------------------------------------------

def _iter_rows_csv(content: bytes):
    """Yield dicts from CSV bytes."""
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]
    missing = REQUIRED_COLS - set(reader.fieldnames)
    if missing:
        raise ValueError(
            f"File is missing required columns: {sorted(missing)}. "
            f"Found: {reader.fieldnames}"
        )
    for row in reader:
        yield {k.strip(): (v.strip() if isinstance(v, str) else v)
               for k, v in row.items()}


def _iter_rows_xlsx(content: bytes):
    """Yield dicts from XLSX bytes using openpyxl."""
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active

    rows = ws.iter_rows(values_only=True)
    header_row = next(rows, None)
    if header_row is None:
        raise ValueError("XLSX file appears to be empty.")

    headers = [_to_str(h) for h in header_row]
    missing = REQUIRED_COLS - set(headers)
    if missing:
        raise ValueError(
            f"File is missing required columns: {sorted(missing)}. "
            f"Found: {headers}"
        )

    for row in rows:
        yield dict(zip(headers, row))

    wb.close()


# ---------------------------------------------------------------------------
# Core invoice parser — works on any row iterator
# ---------------------------------------------------------------------------

def _parse_rows(row_iter) -> tuple[list[ParsedInvoice], dict]:
    invoices: list[ParsedInvoice] = []
    current: Optional[ParsedInvoice] = None
    stats = {"total_rows": 0, "blank_rows": 0, "line_rows": 0, "header_rows": 0}

    for row in row_iter:
        stats["total_rows"] += 1

        bill_no    = _to_str(row.get(COL_BILL_NO, ""))
        alias      = _to_str(row.get(COL_ALIAS, ""))
        amount_raw = row.get(COL_AMOUNT)

        # Completely blank row — skip
        if not bill_no and not alias and (amount_raw is None or _to_str(amount_raw) == ""):
            stats["blank_rows"] += 1
            continue

        # New invoice header row
        if bill_no:
            stats["header_rows"] += 1
            particulars = _to_str(row.get(COL_PARTICULARS, ""))

            # Detect self consumption — skip this invoice entirely
            if SELF_CONSUMPTION in particulars.lower():
                current = None
                continue

            referred_by_raw    = _to_str(row.get(COL_REFERRED_BY, ""))
            referred_by_mobile = _extract_mobile_from_referred_by(referred_by_raw)
            if referred_by_mobile:
                referred_by_mobile = _clean_mobile(referred_by_mobile)

            party_mobile_raw = _to_str(row.get(COL_PARTY_MOBILE, ""))
            party_mobile     = _clean_mobile(party_mobile_raw) if party_mobile_raw else ""

            # Determine provisional invoice type
            if referred_by_mobile:
                inv_type = "contractor_referred"
            elif particulars.lower() not in ("cash", ""):
                inv_type = "contractor_direct"
            else:
                inv_type = "walk_in"

            current = ParsedInvoice(
                invoice_date       = _parse_date(row.get(COL_DATE)),
                bill_number        = bill_no,
                particulars        = particulars,
                party_name         = _to_str(row.get(COL_PARTY_NAME, "")),
                party_mobile       = party_mobile,
                referred_by_raw    = referred_by_raw,
                referred_by_mobile = referred_by_mobile,
                invoice_type       = inv_type,
                contractor_id      = None,
            )
            invoices.append(current)

        # Line item row
        if alias and current is not None:
            stats["line_rows"] += 1
            line = ParsedLine(
                item_code   = alias[:100],
                item_name   = _to_str(row.get(COL_ITEM_DETAILS, ""))[:255],
                quantity    = _parse_float(row.get(COL_QTY)),
                unit        = _to_str(row.get(COL_UNIT, ""))[:20],
                unit_price  = _parse_float(row.get(COL_PRICE)),
                line_amount = _parse_float(row.get(COL_AMOUNT)),
            )
            current.lines.append(line)

    return invoices, stats


# ---------------------------------------------------------------------------
# Public parse function — auto-detects CSV vs XLSX
# ---------------------------------------------------------------------------

def parse_file(content: bytes, filename: str) -> tuple[list[ParsedInvoice], dict]:
    """
    Parse Busy 21 export (CSV or XLSX) into ParsedInvoice objects.
    Returns (invoices, stats).
    """
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    if ext == "xlsx":
        row_iter = _iter_rows_xlsx(content)
    else:
        row_iter = _iter_rows_csv(content)
    return _parse_rows(row_iter)


# ---------------------------------------------------------------------------
# Contractor resolver
# ---------------------------------------------------------------------------

def resolve_contractors(invoices: list[ParsedInvoice], db_conn) -> None:
    """
    Resolves contractor_id for each invoice by querying the DB.
    Mutates invoices in place.
    """
    cursor = db_conn.cursor(dictionary=True)
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
            contractor = code_map.get(inv.particulars)
            if not contractor and inv.party_mobile:
                contractor = mobile_map.get(inv.party_mobile)
            if not contractor:
                inv.invoice_type = "walk_in"

        elif inv.invoice_type == "walk_in":
            if inv.party_mobile:
                contractor = mobile_map.get(inv.party_mobile)
                if contractor:
                    inv.invoice_type = "contractor_direct"

        if contractor:
            inv.contractor_id = contractor["id"]