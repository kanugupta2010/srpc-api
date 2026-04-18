"""Party service — creates parties and auto-provisions their ledgers.

A Party always has exactly one primary Ledger. The ledger is placed
under:
    * Sundry Debtors — for CUSTOMER
    * Sundry Creditors — for SUPPLIER
    * Sundry Debtors — for BOTH (sales is the primary direction; the
      party can still transact as a supplier, it just posts into the
      same ledger with the sign flipped)

This convention keeps the AR/AP reports clean. Parties that genuinely
need two separate ledgers (rare) can be created as two parties.

Opening balances are handled via an auto-posted JOURNAL voucher,
because creating a ledger with a non-zero opening_balance column
WITHOUT a corresponding voucher entry would break the Trial Balance.
The journal uses a reserved 'Opening Balance Equity' ledger (auto-
created under Capital Account on first use).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

from ..db import tx
from ..errors import DomainError
from ..tenancy import get_active_org_id
from .posting_service import (
    VoucherInput,
    VoucherLineInput,
    _post_voucher_inline,
)


class PartyError(DomainError):
    code = "PARTY_ERROR"


class PartyDuplicate(PartyError):
    code = "PARTY_DUPLICATE"


_PARTY_GROUP = {
    "CUSTOMER": "Sundry Debtors",
    "SUPPLIER": "Sundry Creditors",
    "BOTH":     "Sundry Debtors",
}

OPENING_BALANCE_LEDGER_NAME = "Opening Balance Equity"
OPENING_BALANCE_GROUP_NAME = "Capital Account"


@dataclass(frozen=True)
class CreatePartyInput:
    party_type: str
    name: str
    display_name: Optional[str] = None
    mobile: Optional[str] = None
    email: Optional[str] = None
    gstin: Optional[str] = None
    pan: Optional[str] = None
    state_code: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    pincode: Optional[str] = None
    credit_limit: Optional[Decimal] = None
    credit_days: Optional[int] = None
    opening_balance: Decimal = Decimal("0.00")
    opening_balance_dr_cr: str = "Dr"  # ignored if opening_balance == 0


@dataclass(frozen=True)
class CreatePartyOutput:
    party_id: int
    ledger_id: int
    opening_voucher_id: Optional[int] = None


def create_party(payload: CreatePartyInput) -> CreatePartyOutput:
    """Create a party + ledger + optional opening-balance journal."""
    if payload.party_type not in _PARTY_GROUP:
        raise PartyError(
            f"party_type must be CUSTOMER, SUPPLIER or BOTH; got "
            f"{payload.party_type!r}",
            party_type=payload.party_type,
        )

    target_group_name = _PARTY_GROUP[payload.party_type]

    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            # Resolve the target group_id.
            cur.execute(
                "SELECT id FROM account_groups "
                "WHERE organization_id = %(org_id)s AND name = %(name)s "
                "  AND is_reserved = 1",
                {"org_id": get_active_org_id(), "name": target_group_name},
            )
            row = cur.fetchone()
            if not row:
                raise PartyError(
                    f"Reserved group {target_group_name!r} missing. "
                    "Run scripts/seed_phase_a.py first.",
                    group_name=target_group_name,
                )
            group_id = int(row["id"])

            # Reject duplicate name within (org, party_type).
            cur.execute(
                "SELECT id FROM parties "
                "WHERE organization_id = %(org_id)s "
                "  AND party_type = %(pt)s AND name = %(name)s",
                {
                    "org_id": get_active_org_id(),
                    "pt": payload.party_type,
                    "name": payload.name,
                },
            )
            if cur.fetchone():
                raise PartyDuplicate(
                    f"A {payload.party_type} named {payload.name!r} "
                    "already exists.",
                    name=payload.name,
                    party_type=payload.party_type,
                )

            # 1. Insert the ledger (party_id = NULL for now; we'll update
            #    after the party insert).
            cur.execute(
                "INSERT INTO ledgers ("
                "    organization_id, group_id, name, is_party, gstin, "
                "    is_bank, is_reserved, is_active"
                ") VALUES ("
                "    %(org_id)s, %(gid)s, %(name)s, 1, %(gstin)s, "
                "    0, 0, 1"
                ")",
                {
                    "org_id": get_active_org_id(),
                    "gid": group_id,
                    "name": payload.name,
                    "gstin": payload.gstin,
                },
            )
            ledger_id = int(cur.lastrowid)

            # 2. Insert the party with the ledger link.
            cur.execute(
                "INSERT INTO parties ("
                "    organization_id, party_type, name, display_name, "
                "    mobile, email, gstin, pan, state_code, "
                "    address_line1, address_line2, city, pincode, "
                "    credit_limit, credit_days, ledger_id, is_active"
                ") VALUES ("
                "    %(org_id)s, %(pt)s, %(name)s, %(dn)s, %(m)s, %(e)s, "
                "    %(g)s, %(p)s, %(sc)s, %(a1)s, %(a2)s, %(c)s, %(pc)s, "
                "    %(cl)s, %(cd)s, %(lid)s, 1"
                ")",
                {
                    "org_id": get_active_org_id(),
                    "pt": payload.party_type,
                    "name": payload.name,
                    "dn": payload.display_name,
                    "m": payload.mobile,
                    "e": payload.email,
                    "g": payload.gstin,
                    "p": payload.pan,
                    "sc": payload.state_code,
                    "a1": payload.address_line1,
                    "a2": payload.address_line2,
                    "c": payload.city,
                    "pc": payload.pincode,
                    "cl": payload.credit_limit,
                    "cd": payload.credit_days,
                    "lid": ledger_id,
                },
            )
            party_id = int(cur.lastrowid)

            # 3. Update the ledger's party_id (reverse link).
            cur.execute(
                "UPDATE ledgers SET party_id = %(pid)s "
                "WHERE organization_id = %(org_id)s AND id = %(lid)s",
                {
                    "org_id": get_active_org_id(),
                    "pid": party_id,
                    "lid": ledger_id,
                },
            )
        finally:
            cur.close()

        # 4. Opening balance via a JOURNAL voucher if requested.
        opening_voucher_id: Optional[int] = None
        if payload.opening_balance and payload.opening_balance != Decimal("0"):
            ob_ledger_id = _ensure_opening_balance_ledger(conn)
            party_side = payload.opening_balance_dr_cr
            equity_side = "Cr" if party_side == "Dr" else "Dr"

            posted = _post_voucher_inline(
                conn,
                VoucherInput(
                    voucher_type="JOURNAL",
                    voucher_date=date.today(),
                    lines=[
                        VoucherLineInput(
                            ledger_id=ledger_id,
                            dr_cr=party_side,
                            amount=Decimal(payload.opening_balance),
                            line_narration="Opening balance",
                        ),
                        VoucherLineInput(
                            ledger_id=ob_ledger_id,
                            dr_cr=equity_side,
                            amount=Decimal(payload.opening_balance),
                            line_narration=f"Opening balance for {payload.name}",
                        ),
                    ],
                    party_ledger_id=ledger_id,
                    narration=f"Opening balance for {payload.name}",
                    source_doc_type="PARTY_OPENING",
                    source_doc_id=party_id,
                ),
            )
            opening_voucher_id = posted.voucher_id

        return CreatePartyOutput(
            party_id=party_id,
            ledger_id=ledger_id,
            opening_voucher_id=opening_voucher_id,
        )


def _ensure_opening_balance_ledger(conn) -> int:
    """Find or create the 'Opening Balance Equity' ledger."""
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id FROM ledgers "
            "WHERE organization_id = %(org_id)s "
            "  AND name = %(name)s",
            {
                "org_id": get_active_org_id(),
                "name": OPENING_BALANCE_LEDGER_NAME,
            },
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])

        cur.execute(
            "SELECT id FROM account_groups "
            "WHERE organization_id = %(org_id)s "
            "  AND name = %(name)s AND is_reserved = 1",
            {
                "org_id": get_active_org_id(),
                "name": OPENING_BALANCE_GROUP_NAME,
            },
        )
        grp = cur.fetchone()
        if not grp:
            raise PartyError(
                f"Reserved group {OPENING_BALANCE_GROUP_NAME!r} missing; "
                "run scripts/seed_phase_a.py."
            )

        cur.execute(
            "INSERT INTO ledgers ("
            "    organization_id, group_id, name, is_reserved, is_active"
            ") VALUES ("
            "    %(org_id)s, %(gid)s, %(name)s, 1, 1"
            ")",
            {
                "org_id": get_active_org_id(),
                "gid": int(grp["id"]),
                "name": OPENING_BALANCE_LEDGER_NAME,
            },
        )
        return int(cur.lastrowid)
    finally:
        cur.close()
