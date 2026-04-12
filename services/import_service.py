"""
services/import_service.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Invoice parser — reads Busy 21 XLSX or CSV export.

Two separate identifiers per invoice:
  invoice_type  → nature of transaction: sale | sale_return | internal
  customer_type → who the customer is:   contractor_direct | contractor_referred | walk_in | not_applicable

Busy 21 export columns:
  Date | Vch Type | Vch/Bill No | Particulars | Alias | Item Details |
  Qty. | Unit | Price | Amount | Party Name | Party Mobile | Referred By
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
# Column constants
# ---------------------------------------------------------------------------
COL_DATE         = "Date"
COL_VCH_TYPE     = "Vch Type"
COL_BILL_NO      = "Vch/Bill No"
COL_PARTICULARS  = "Particulars"
COL_ALIAS        = "Alias"
COL_ITEM_DETAILS = "Item Details"
COL_QTY          = "Qty."
COL_QTY_ALT      = "Qty"
COL_UNIT         = "Unit"
COL_PRICE        = "Price"
COL_AMOUNT       = "Amount"
COL_PARTY_NAME   = "Party Name"
COL_PARTY_MOBILE = "Party Mobile"
COL_REFERRED_BY  = "Referred By"

REQUIRED_COLS = {COL_BILL_NO, COL_ALIAS, COL_AMOUNT}

# Vch Type values from Busy 21
VCH_SALE   = "Sale"
VCH_RETURN = "SlRt"

# invoice_type values
INV_SALE        = "sale"
INV_SALE_RETURN = "sale_return"
INV_INTERNAL    = "internal"

# customer_type values
CUST_CONTRACTOR_DIRECT   = "contractor_direct"
CUST_CONTRACTOR_REFERRED = "contractor_referred"
CUST_WALK_IN             = "walk_in"
CUST_NOT_APPLICABLE      = "not_applicable"

REFERRED_BY_RE = re.compile(r"-\s*(\d{10,12})\s*$")


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
    invoice_type:       str            # sale | sale_return | internal
    customer_type:      str            # contractor_direct | contractor_referred | walk_in | not_applicable
    particulars:        str
    party_name:         str
    party_mobile:       str
    referred_by_raw:    str
    referred_by_mobile: Optional[str]
    contractor_id:      Optional[int]
    lines:              list = field(default_factory=list)

    @property
    def is_return(self) -> bool:
        return self.invoice_type == INV_SALE_RETURN

    @property
    def gross_amount(self) -> float:
        return sum(l.line_amount for l in self.lines)


# ---------------------------------------------------------------------------
# Value parsers
# ---------------------------------------------------------------------------

def _parse_date(raw) -> Optional[date]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    v = str(raw).strip()
    if not v:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y",
                "%d %b %Y", "%d-%m-%y", "%d/%m/%y"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    log.warning("Could not parse date '%s'", raw)
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
    return "" if raw is None else str(raw).strip()


def _extract_mobile_from_referred_by(raw: str) -> Optional[str]:
    if not raw:
        return None
    m = REFERRED_BY_RE.search(raw.strip())
    return m.group(1) if m else None


def _clean_mobile(raw: str) -> str:
    v = raw.strip().replace(" ", "").replace("-", "")
    if v.startswith("+91"):
        v = v[3:]
    elif v.startswith("91") and len(v) == 12:
        v = v[2:]
    if v.startswith("0") and len(v) == 11:
        v = v[1:]
    return v


def _get_qty(row: dict) -> float:
    return _parse_float(row.get(COL_QTY) or row.get(COL_QTY_ALT))


# ---------------------------------------------------------------------------
# Row iterators
# ---------------------------------------------------------------------------

def _iter_rows_xlsx(content: bytes):
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    header_row = next(rows, None)
    if header_row is None:
        raise ValueError("XLSX file is empty.")
    headers = [_to_str(h) for h in header_row]
    missing = REQUIRED_COLS - set(headers)
    if missing:
        raise ValueError(f"File missing required columns: {sorted(missing)}. Found: {headers}")
    for row in rows:
        yield dict(zip(headers, row))
    wb.close()


def _iter_rows_csv(content: bytes):
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]
    missing = REQUIRED_COLS - set(reader.fieldnames)
    if missing:
        raise ValueError(f"File missing required columns: {sorted(missing)}. Found: {reader.fieldnames}")
    for row in reader:
        yield {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


# ---------------------------------------------------------------------------
# Core parser
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
        vch_type   = _to_str(row.get(COL_VCH_TYPE, ""))

        # Skip completely blank rows
        if not bill_no and not alias and (amount_raw is None or _to_str(amount_raw) == ""):
            stats["blank_rows"] += 1
            continue

        # New invoice header row
        if bill_no:
            stats["header_rows"] += 1
            particulars = _to_str(row.get(COL_PARTICULARS, ""))

            referred_by_raw    = _to_str(row.get(COL_REFERRED_BY, ""))
            referred_by_mobile = _extract_mobile_from_referred_by(referred_by_raw)
            if referred_by_mobile:
                referred_by_mobile = _clean_mobile(referred_by_mobile)

            party_mobile_raw = _to_str(row.get(COL_PARTY_MOBILE, ""))
            party_mobile     = _clean_mobile(party_mobile_raw) if party_mobile_raw else ""

            # --- Determine invoice_type ---
            if vch_type == VCH_RETURN:
                invoice_type = INV_SALE_RETURN
            elif particulars.lower() == "self consumption of goods":
                invoice_type = INV_INTERNAL
            else:
                invoice_type = INV_SALE

            # --- Determine customer_type ---
            if invoice_type == INV_INTERNAL:
                customer_type = CUST_NOT_APPLICABLE
            elif referred_by_mobile:
                customer_type = CUST_CONTRACTOR_REFERRED
            elif particulars.lower() not in ("cash", ""):
                customer_type = CUST_CONTRACTOR_DIRECT
            else:
                customer_type = CUST_WALK_IN

            current = ParsedInvoice(
                invoice_date       = _parse_date(row.get(COL_DATE)),
                bill_number        = bill_no,
                invoice_type       = invoice_type,
                customer_type      = customer_type,
                particulars        = particulars,
                party_name         = _to_str(row.get(COL_PARTY_NAME, "")),
                party_mobile       = party_mobile,
                referred_by_raw    = referred_by_raw,
                referred_by_mobile = referred_by_mobile,
                contractor_id      = None,
            )
            invoices.append(current)

        # Line item row
        if alias and current is not None:
            stats["line_rows"] += 1
            current.lines.append(ParsedLine(
                item_code   = alias[:100],
                item_name   = _to_str(row.get(COL_ITEM_DETAILS, ""))[:255],
                quantity    = _get_qty(row),
                unit        = _to_str(row.get(COL_UNIT, ""))[:20],
                unit_price  = _parse_float(row.get(COL_PRICE)),
                line_amount = _parse_float(row.get(COL_AMOUNT)),
            ))

    return invoices, stats


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def parse_file(content: bytes, filename: str) -> tuple[list[ParsedInvoice], dict]:
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    row_iter = _iter_rows_xlsx(content) if ext == "xlsx" else _iter_rows_csv(content)
    return _parse_rows(row_iter)


# ---------------------------------------------------------------------------
# Contractor resolver
# ---------------------------------------------------------------------------

def resolve_contractors(invoices: list[ParsedInvoice], db_conn) -> None:
    """Resolves contractor_id for each invoice. Mutates in place."""
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
        # internal invoices don't need contractor resolution
        if inv.invoice_type == INV_INTERNAL:
            continue

        contractor = None

        if inv.customer_type == CUST_CONTRACTOR_REFERRED:
            contractor = mobile_map.get(inv.referred_by_mobile or "")

        elif inv.customer_type == CUST_CONTRACTOR_DIRECT:
            contractor = code_map.get(inv.particulars)
            if not contractor and inv.party_mobile:
                contractor = mobile_map.get(inv.party_mobile)
            if not contractor:
                inv.customer_type = CUST_WALK_IN

        elif inv.customer_type == CUST_WALK_IN:
            if inv.party_mobile:
                contractor = mobile_map.get(inv.party_mobile)
                if contractor:
                    inv.customer_type = CUST_CONTRACTOR_DIRECT

        # For sale returns — same logic, try referred_by as fallback
        if not contractor and inv.is_return and inv.referred_by_mobile:
            contractor = mobile_map.get(inv.referred_by_mobile)

        if contractor:
            inv.contractor_id = contractor["id"]