"""Masters endpoints — parties and ledgers.

    POST /api/v1/masters/parties         — create party + auto-ledger
    GET  /api/v1/masters/parties         — list parties
    GET  /api/v1/masters/parties/{id}    — one party
    GET  /api/v1/masters/ledgers         — list ledgers (filterable)
"""
from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status

from ..db import tx
from ..repos.ledgers import LedgerRepository
from ..schemas.party_schemas import (
    CreatePartyRequest,
    LedgerResponse,
    PartyResponse,
)
from ..services.party_service import CreatePartyInput, create_party
from .deps import bind_active_org, current_admin
from ..tenancy import get_active_org_id

router = APIRouter(prefix="/api/v1/masters", tags=["Masters"])


# ---------------------------------------------------------------------------
# POST /api/v1/masters/parties
# ---------------------------------------------------------------------------

@router.post(
    "/parties",
    response_model=PartyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a party (customer/supplier) + auto-link ledger",
)
def create_party_endpoint(
    req: CreatePartyRequest,
    _org: int = Depends(bind_active_org),
    _admin: str = Depends(current_admin),
):
    payload = CreatePartyInput(
        party_type=req.party_type,
        name=req.name,
        display_name=req.display_name,
        mobile=req.mobile,
        email=req.email,
        gstin=req.gstin,
        pan=req.pan,
        state_code=req.state_code,
        address_line1=req.address_line1,
        address_line2=req.address_line2,
        city=req.city,
        pincode=req.pincode,
        credit_limit=(
            Decimal(req.credit_limit) if req.credit_limit is not None else None
        ),
        credit_days=req.credit_days,
        opening_balance=Decimal(req.opening_balance),
        opening_balance_dr_cr=req.opening_balance_dr_cr,
    )
    result = create_party(payload)

    # Re-fetch for the response body.
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT p.id, p.party_type, p.name, p.display_name, "
                "       p.mobile, p.email, p.gstin, p.state_code, "
                "       p.ledger_id, l.name AS ledger_name, p.is_active "
                "FROM parties p "
                "LEFT JOIN ledgers l "
                "       ON l.id = p.ledger_id "
                "      AND l.organization_id = p.organization_id "
                "WHERE p.organization_id = %(org_id)s AND p.id = %(pid)s",
                {"org_id": get_active_org_id(), "pid": result.party_id},
            )
            row = cur.fetchone()
        finally:
            cur.close()

    return PartyResponse(
        id=int(row["id"]),
        party_type=row["party_type"],
        name=row["name"],
        display_name=row["display_name"],
        mobile=row["mobile"],
        email=row["email"],
        gstin=row["gstin"],
        state_code=row["state_code"],
        ledger_id=int(row["ledger_id"]) if row["ledger_id"] is not None else None,
        ledger_name=row["ledger_name"],
        is_active=bool(row["is_active"]),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/masters/parties
# ---------------------------------------------------------------------------

@router.get(
    "/parties",
    response_model=list[PartyResponse],
    summary="List parties",
)
def list_parties(
    party_type: Annotated[
        Optional[Literal["CUSTOMER", "SUPPLIER", "BOTH"]], Query()
    ] = None,
    search: Annotated[Optional[str], Query(max_length=100)] = None,
    _org: int = Depends(bind_active_org),
):
    clauses = ["p.organization_id = %(org_id)s", "p.is_active = 1"]
    params: dict = {"org_id": get_active_org_id()}
    if party_type:
        clauses.append("p.party_type = %(pt)s")
        params["pt"] = party_type
    if search:
        clauses.append("(p.name LIKE %(s)s OR p.mobile LIKE %(s)s)")
        params["s"] = f"%{search}%"

    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT p.id, p.party_type, p.name, p.display_name, "
                "       p.mobile, p.email, p.gstin, p.state_code, "
                "       p.ledger_id, l.name AS ledger_name, p.is_active "
                "FROM parties p "
                "LEFT JOIN ledgers l "
                "       ON l.id = p.ledger_id "
                "      AND l.organization_id = p.organization_id "
                "WHERE " + " AND ".join(clauses) + " "
                "ORDER BY p.name LIMIT 500",
                params,
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        PartyResponse(
            id=int(r["id"]),
            party_type=r["party_type"],
            name=r["name"],
            display_name=r["display_name"],
            mobile=r["mobile"],
            email=r["email"],
            gstin=r["gstin"],
            state_code=r["state_code"],
            ledger_id=(
                int(r["ledger_id"]) if r["ledger_id"] is not None else None
            ),
            ledger_name=r["ledger_name"],
            is_active=bool(r["is_active"]),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# GET /api/v1/masters/parties/{id}
# ---------------------------------------------------------------------------

@router.get(
    "/parties/{party_id}",
    response_model=PartyResponse,
    summary="Get one party",
)
def get_party(
    party_id: Annotated[int, Path(gt=0)],
    _org: int = Depends(bind_active_org),
):
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT p.id, p.party_type, p.name, p.display_name, "
                "       p.mobile, p.email, p.gstin, p.state_code, "
                "       p.ledger_id, l.name AS ledger_name, p.is_active "
                "FROM parties p "
                "LEFT JOIN ledgers l "
                "       ON l.id = p.ledger_id "
                "      AND l.organization_id = p.organization_id "
                "WHERE p.organization_id = %(org_id)s AND p.id = %(pid)s",
                {"org_id": get_active_org_id(), "pid": party_id},
            )
            r = cur.fetchone()
        finally:
            cur.close()

    if not r:
        raise HTTPException(
            status_code=404,
            detail={"code": "PARTY_NOT_FOUND", "message": "Party not found."},
        )
    return PartyResponse(
        id=int(r["id"]),
        party_type=r["party_type"],
        name=r["name"],
        display_name=r["display_name"],
        mobile=r["mobile"],
        email=r["email"],
        gstin=r["gstin"],
        state_code=r["state_code"],
        ledger_id=int(r["ledger_id"]) if r["ledger_id"] is not None else None,
        ledger_name=r["ledger_name"],
        is_active=bool(r["is_active"]),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/masters/ledgers
# ---------------------------------------------------------------------------

@router.get(
    "/ledgers",
    response_model=list[LedgerResponse],
    summary="List ledgers, optionally filtered by group or flags",
)
def list_ledgers(
    is_bank: Annotated[Optional[bool], Query()] = None,
    is_party: Annotated[Optional[bool], Query()] = None,
    group_name: Annotated[Optional[str], Query(max_length=120)] = None,
    _org: int = Depends(bind_active_org),
):
    clauses = [
        "l.organization_id = %(org_id)s",
        "l.is_active = 1",
    ]
    params: dict = {"org_id": get_active_org_id()}
    if is_bank is not None:
        clauses.append("l.is_bank = %(isb)s")
        params["isb"] = 1 if is_bank else 0
    if is_party is not None:
        clauses.append("l.is_party = %(isp)s")
        params["isp"] = 1 if is_party else 0
    if group_name:
        clauses.append("g.name = %(gn)s")
        params["gn"] = group_name

    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT l.id, l.group_id, g.name AS group_name, l.name, "
                "       l.opening_balance, l.is_party, l.is_bank, "
                "       l.is_reserved, l.is_active "
                "FROM ledgers l "
                "JOIN account_groups g "
                "       ON g.id = l.group_id "
                "      AND g.organization_id = l.organization_id "
                "WHERE " + " AND ".join(clauses) + " "
                "ORDER BY g.name, l.name LIMIT 1000",
                params,
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        LedgerResponse(
            id=int(r["id"]),
            group_id=int(r["group_id"]),
            group_name=r["group_name"],
            name=r["name"],
            opening_balance=Decimal(r["opening_balance"]),
            is_party=bool(r["is_party"]),
            is_bank=bool(r["is_bank"]),
            is_reserved=bool(r["is_reserved"]),
            is_active=bool(r["is_active"]),
        )
        for r in rows
    ]
