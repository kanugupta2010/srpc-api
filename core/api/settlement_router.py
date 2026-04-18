"""Settlement endpoints.

    POST /api/v1/settlement/allocate     — allocate a posted voucher to bills
    GET  /api/v1/settlement/outstanding  — list open/partial bills
    POST /api/v1/settlement/bills/opening — seed a bill_reference without invoice

The opening-bill endpoint is a temporary bootstrap for migrating
outstanding AR/AP from Busy. Phase E will remove the need for it by
creating bill_references as a side-effect of posting invoices.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Depends, Query, status

from ..db import tx
from ..repos.bill_references import BillReferenceRepository
from ..repos.ledgers import LedgerRepository
from ..schemas.settlement_schemas import (
    AllocateRequest,
    BillReferenceResponse,
    OpeningBillRequest,
)
from ..services.settlement_service import (
    AllocationInput,
    allocate,
    create_opening_bill,
)
from .deps import bind_active_org, current_admin

router = APIRouter(prefix="/api/v1/settlement", tags=["Settlement"])


# ---------------------------------------------------------------------------
# POST /api/v1/settlement/allocate
# ---------------------------------------------------------------------------

@router.post(
    "/allocate",
    status_code=status.HTTP_201_CREATED,
    summary="Allocate a posted Receipt/Payment/CN/DN to one or more bills",
)
def do_allocate(
    req: AllocateRequest,
    _org: int = Depends(bind_active_org),
    _admin: str = Depends(current_admin),
):
    inputs = [
        AllocationInput(
            bill_reference_id=a.bill_reference_id,
            amount=Decimal(a.amount),
        )
        for a in req.allocations
    ]
    result = allocate(req.allocating_voucher_id, inputs)
    return {
        "allocating_voucher_id": result.allocating_voucher_id,
        "allocation_ids": result.allocation_ids,
        "affected_bill_ids": result.affected_bill_ids,
    }


# ---------------------------------------------------------------------------
# GET /api/v1/settlement/outstanding
# ---------------------------------------------------------------------------

@router.get(
    "/outstanding",
    response_model=list[BillReferenceResponse],
    summary="List outstanding bills (OPEN or PARTIAL)",
)
def list_outstanding(
    party_ledger_id: Annotated[Optional[int], Query(gt=0)] = None,
    side: Annotated[
        Optional[Literal["RECEIVABLE", "PAYABLE"]],
        Query(),
    ] = None,
    _org: int = Depends(bind_active_org),
):
    with tx() as conn:
        bill_repo = BillReferenceRepository(conn)
        ledger_repo = LedgerRepository(conn)
        bills = bill_repo.list_outstanding(
            party_ledger_id=party_ledger_id,
            side=side,
        )

        # Enrich with party name for UX.
        out: list[BillReferenceResponse] = []
        name_cache: dict[int, str] = {}
        for b in bills:
            lid = int(b["party_ledger_id"])
            if lid not in name_cache:
                row = ledger_repo.get_by_id(lid)
                name_cache[lid] = row["name"] if row else ""
            out.append(
                BillReferenceResponse(
                    id=int(b["id"]),
                    party_ledger_id=lid,
                    party_ledger_name=name_cache[lid],
                    bill_no=b["bill_no"],
                    bill_date=b["bill_date"],
                    due_date=b["due_date"],
                    original_amount=Decimal(b["original_amount"]),
                    outstanding_amount=Decimal(b["outstanding_amount"]),
                    side=b["side"],
                    status=b["status"],
                    source_voucher_id=(
                        int(b["source_voucher_id"])
                        if b["source_voucher_id"] is not None else None
                    ),
                )
            )
        return out


# ---------------------------------------------------------------------------
# POST /api/v1/settlement/bills/opening
# ---------------------------------------------------------------------------

@router.post(
    "/bills/opening",
    status_code=status.HTTP_201_CREATED,
    summary="Seed a bill_reference without a source invoice (pre-Phase E)",
)
def create_opening(
    req: OpeningBillRequest,
    _org: int = Depends(bind_active_org),
    _admin: str = Depends(current_admin),
):
    bill_id = create_opening_bill(
        party_ledger_id=req.party_ledger_id,
        bill_no=req.bill_no,
        bill_date=req.bill_date,
        original_amount=Decimal(req.amount),
        side=req.side,
        due_date=req.due_date,
        notes=req.notes,
    )
    return {"bill_reference_id": bill_id}
