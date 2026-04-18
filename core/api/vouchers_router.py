"""Voucher endpoints for the accounting core.

    POST /api/v1/vouchers/payment    — money out (Dr expense/creditor, Cr cash/bank)
    POST /api/v1/vouchers/receipt    — money in  (Dr cash/bank, Cr debtor/income)
    POST /api/v1/vouchers/journal    — generic Dr/Cr multi-line
    POST /api/v1/vouchers/contra     — cash <-> bank (or bank <-> bank)
    GET  /api/v1/vouchers/{id}       — full voucher + lines
    POST /api/v1/vouchers/{id}/cancel — cancel + auto-reverse allocations

Auth: require_admin + bind_active_org (wrapped in one dep).

Each endpoint is a thin translator from the Pydantic request shape to
the posting_service.VoucherInput DTO. All invariants live in the
service; this router just marshals.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status

from ..db import tx
from ..repos.vouchers import VoucherRepository
from ..repos.ledgers import LedgerRepository
from ..schemas.voucher_schemas import (
    CancelVoucherRequest,
    ContraVoucherRequest,
    JournalVoucherRequest,
    PaymentVoucherRequest,
    PostedVoucherResponse,
    ReceiptVoucherRequest,
    VoucherLineResponse,
    VoucherResponse,
)
from ..services.posting_service import (
    VoucherInput,
    VoucherLineInput,
    _post_voucher_inline,
)
from ..services.settlement_service import (
    AllocationInput,
    _allocate_inline,
)
from ..services.voucher_cancel_service import (
    _audit,
    cancel_voucher_with_allocations,
)
from .deps import bind_active_org, current_admin

router = APIRouter(prefix="/api/v1/vouchers", tags=["Vouchers"])


# ---------------------------------------------------------------------------
# POST /api/v1/vouchers/payment
# ---------------------------------------------------------------------------

@router.post(
    "/payment",
    response_model=PostedVoucherResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Post a Payment voucher (money out)",
)
def post_payment(
    req: PaymentVoucherRequest,
    _org: int = Depends(bind_active_org),
    admin: str = Depends(current_admin),
):
    total = sum((Decimal(ln.amount) for ln in req.lines), Decimal("0"))
    dr_lines = [
        VoucherLineInput(
            ledger_id=ln.ledger_id,
            dr_cr="Dr",
            amount=Decimal(ln.amount),
            cost_center_id=ln.cost_center_id,
            line_narration=ln.line_narration,
        )
        for ln in req.lines
    ]
    cr_line = VoucherLineInput(
        ledger_id=req.paid_from_ledger_id,
        dr_cr="Cr",
        amount=total,
        line_narration=req.narration or "Payment",
    )

    payload = VoucherInput(
        voucher_type="PAYMENT",
        voucher_date=req.voucher_date,
        lines=[*dr_lines, cr_line],
        party_ledger_id=req.party_ledger_id,
        reference_no=req.reference_no,
        narration=req.narration,
        created_by=admin,
    )

    with tx() as conn:
        posted = _post_voucher_inline(conn, payload)
        _audit(
            conn,
            entity_type="voucher",
            entity_id=posted.voucher_id,
            action="POSTED",
            actor=admin,
            details={
                "voucher_number": posted.voucher_number,
                "voucher_type": "PAYMENT",
                "total_amount": str(posted.total_amount),
            },
        )

    return PostedVoucherResponse(
        voucher_id=posted.voucher_id,
        voucher_number=posted.voucher_number,
        voucher_type=posted.voucher_type,
        total_amount=posted.total_amount,
    )


# ---------------------------------------------------------------------------
# POST /api/v1/vouchers/receipt
# ---------------------------------------------------------------------------

@router.post(
    "/receipt",
    response_model=PostedVoucherResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Post a Receipt voucher (money in), optionally with bill allocation",
)
def post_receipt(
    req: ReceiptVoucherRequest,
    _org: int = Depends(bind_active_org),
    admin: str = Depends(current_admin),
):
    total = sum((Decimal(ln.amount) for ln in req.lines), Decimal("0"))
    cr_lines = [
        VoucherLineInput(
            ledger_id=ln.ledger_id,
            dr_cr="Cr",
            amount=Decimal(ln.amount),
            cost_center_id=ln.cost_center_id,
            line_narration=ln.line_narration,
        )
        for ln in req.lines
    ]
    dr_line = VoucherLineInput(
        ledger_id=req.received_into_ledger_id,
        dr_cr="Dr",
        amount=total,
        line_narration=req.narration or "Receipt",
    )

    payload = VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=req.voucher_date,
        lines=[dr_line, *cr_lines],
        party_ledger_id=req.party_ledger_id,
        reference_no=req.reference_no,
        narration=req.narration,
        created_by=admin,
    )

    with tx() as conn:
        posted = _post_voucher_inline(conn, payload)

        # Optional inline allocation.
        if req.allocate_to_bills:
            if req.party_ledger_id is None:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "ALLOCATION_NEEDS_PARTY",
                        "message": "party_ledger_id is required when "
                                   "allocate_to_bills is set.",
                    },
                )
            alloc_inputs = [
                AllocationInput(
                    bill_reference_id=a.bill_reference_id,
                    amount=Decimal(a.amount),
                )
                for a in req.allocate_to_bills
            ]
            _allocate_inline(conn, posted.voucher_id, alloc_inputs)

        _audit(
            conn,
            entity_type="voucher",
            entity_id=posted.voucher_id,
            action="POSTED",
            actor=admin,
            details={
                "voucher_number": posted.voucher_number,
                "voucher_type": "RECEIPT",
                "total_amount": str(posted.total_amount),
                "allocations_count": len(req.allocate_to_bills or []),
            },
        )

    return PostedVoucherResponse(
        voucher_id=posted.voucher_id,
        voucher_number=posted.voucher_number,
        voucher_type=posted.voucher_type,
        total_amount=posted.total_amount,
    )


# ---------------------------------------------------------------------------
# POST /api/v1/vouchers/journal
# ---------------------------------------------------------------------------

@router.post(
    "/journal",
    response_model=PostedVoucherResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Post a Journal voucher (generic multi-line adjustment)",
)
def post_journal(
    req: JournalVoucherRequest,
    _org: int = Depends(bind_active_org),
    admin: str = Depends(current_admin),
):
    payload = VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=req.voucher_date,
        lines=[
            VoucherLineInput(
                ledger_id=ln.ledger_id,
                dr_cr=ln.dr_cr,
                amount=Decimal(ln.amount),
                cost_center_id=ln.cost_center_id,
                line_narration=ln.line_narration,
            )
            for ln in req.lines
        ],
        party_ledger_id=req.party_ledger_id,
        reference_no=req.reference_no,
        narration=req.narration,
        created_by=admin,
    )

    with tx() as conn:
        posted = _post_voucher_inline(conn, payload)
        _audit(
            conn,
            entity_type="voucher",
            entity_id=posted.voucher_id,
            action="POSTED",
            actor=admin,
            details={
                "voucher_number": posted.voucher_number,
                "voucher_type": "JOURNAL",
                "total_amount": str(posted.total_amount),
            },
        )

    return PostedVoucherResponse(
        voucher_id=posted.voucher_id,
        voucher_number=posted.voucher_number,
        voucher_type=posted.voucher_type,
        total_amount=posted.total_amount,
    )


# ---------------------------------------------------------------------------
# POST /api/v1/vouchers/contra
# ---------------------------------------------------------------------------

@router.post(
    "/contra",
    response_model=PostedVoucherResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Post a Contra voucher (cash <-> bank)",
)
def post_contra(
    req: ContraVoucherRequest,
    _org: int = Depends(bind_active_org),
    admin: str = Depends(current_admin),
):
    payload = VoucherInput(
        voucher_type="CONTRA",
        voucher_date=req.voucher_date,
        lines=[
            VoucherLineInput(
                ledger_id=req.to_ledger_id,
                dr_cr="Dr",
                amount=Decimal(req.amount),
                line_narration=req.narration,
            ),
            VoucherLineInput(
                ledger_id=req.from_ledger_id,
                dr_cr="Cr",
                amount=Decimal(req.amount),
                line_narration=req.narration,
            ),
        ],
        reference_no=req.reference_no,
        narration=req.narration,
        created_by=admin,
    )

    with tx() as conn:
        posted = _post_voucher_inline(conn, payload)
        _audit(
            conn,
            entity_type="voucher",
            entity_id=posted.voucher_id,
            action="POSTED",
            actor=admin,
            details={
                "voucher_number": posted.voucher_number,
                "voucher_type": "CONTRA",
                "total_amount": str(posted.total_amount),
            },
        )

    return PostedVoucherResponse(
        voucher_id=posted.voucher_id,
        voucher_number=posted.voucher_number,
        voucher_type=posted.voucher_type,
        total_amount=posted.total_amount,
    )


# ---------------------------------------------------------------------------
# GET /api/v1/vouchers/{id}
# ---------------------------------------------------------------------------

@router.get(
    "/{voucher_id}",
    response_model=VoucherResponse,
    summary="Get a voucher with its lines",
)
def get_voucher(
    voucher_id: Annotated[int, Path(gt=0)],
    _org: int = Depends(bind_active_org),
):
    with tx() as conn:
        voucher_repo = VoucherRepository(conn)
        ledger_repo = LedgerRepository(conn)

        header = voucher_repo.get_header(voucher_id)
        if header is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "VOUCHER_NOT_FOUND", "message": "Voucher not found."},
            )

        lines = voucher_repo.get_lines(voucher_id)

        # Enrich with ledger names for UX.
        ledger_names: dict[int, str] = {}
        for ln in lines:
            lid = int(ln["ledger_id"])
            if lid not in ledger_names:
                row = ledger_repo.get_by_id(lid)
                ledger_names[lid] = row["name"] if row else ""

        party_ledger_name = None
        if header["party_ledger_id"] is not None:
            row = ledger_repo.get_by_id(int(header["party_ledger_id"]))
            party_ledger_name = row["name"] if row else None

    return VoucherResponse(
        id=int(header["id"]),
        voucher_type=header["voucher_type"],
        voucher_number=header["voucher_number"],
        voucher_date=header["voucher_date"],
        reference_no=header["reference_no"],
        party_ledger_id=(
            int(header["party_ledger_id"])
            if header["party_ledger_id"] is not None else None
        ),
        party_ledger_name=party_ledger_name,
        narration=header["narration"],
        total_amount=Decimal(header["total_amount"]),
        status=header["status"],
        source_doc_type=header["source_doc_type"],
        source_doc_id=(
            int(header["source_doc_id"])
            if header["source_doc_id"] is not None else None
        ),
        lines=[
            VoucherLineResponse(
                id=int(ln["id"]),
                ledger_id=int(ln["ledger_id"]),
                ledger_name=ledger_names.get(int(ln["ledger_id"])),
                dr_cr=ln["dr_cr"],
                amount=Decimal(ln["amount"]),
                cost_center_id=(
                    int(ln["cost_center_id"])
                    if ln["cost_center_id"] is not None else None
                ),
                line_narration=ln["line_narration"],
                line_order=int(ln["line_order"]),
            )
            for ln in lines
        ],
    )


# ---------------------------------------------------------------------------
# POST /api/v1/vouchers/{id}/cancel
# ---------------------------------------------------------------------------

@router.post(
    "/{voucher_id}/cancel",
    status_code=status.HTTP_200_OK,
    summary="Cancel a posted voucher (auto-reverses allocations)",
)
def cancel_voucher(
    voucher_id: Annotated[int, Path(gt=0)],
    req: CancelVoucherRequest,
    _org: int = Depends(bind_active_org),
    admin: str = Depends(current_admin),
):
    result = cancel_voucher_with_allocations(
        voucher_id,
        cancelled_by=admin,
        reason=req.reason,
    )
    return {
        "cancelled_voucher_id": result.cancelled_voucher_id,
        "reversing_voucher_id": result.reversing_voucher_id,
        "reversing_voucher_number": result.reversing_voucher_number,
        "reversed_allocation_bill_ids": result.reversed_allocation_bill_ids,
    }
