"""Settlement service — allocates a Receipt/Payment/CN/DN to one or more bills.

Allocation rules (CloudAccountingDesign §7.3):

    R1. For each allocation row: amount <= bill.outstanding_amount
    R2. Sum of allocations for a voucher <= voucher's net impact on the
        party ledger.
    R3. The bill's side must match the voucher's role (RECEIVABLE bills
        can only be allocated by Receipt/CN; PAYABLE by Payment/DN).
    R4. The bill's party_ledger must equal the voucher's party_ledger.

All writes happen in a single transaction. Allocation rows are inserted,
each target bill_reference is locked with SELECT FOR UPDATE, then its
outstanding_amount is recomputed.

This service can be called both:
    * Standalone via POST /api/v1/settlement/allocate (after the voucher
      has already been posted and has unallocated amount 'on account').
    * Inline by the Receipt/Payment voucher creation endpoints when
      the caller supplies allocate_to_bills in the same request.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from ..db import tx
from ..errors import DomainError
from ..repos.allocations import AllocationRepository
from ..repos.bill_references import BillReferenceRepository
from ..repos.vouchers import VoucherRepository

# ----- Allocation-specific errors -----------------------------------------

class AllocationError(DomainError):
    code = "ALLOCATION_ERROR"


class AllocationExceedsBill(AllocationError):
    code = "ALLOCATION_EXCEEDS_BILL"


class AllocationExceedsVoucher(AllocationError):
    code = "ALLOCATION_EXCEEDS_VOUCHER"


class AllocationSideMismatch(AllocationError):
    code = "ALLOCATION_SIDE_MISMATCH"


class AllocationPartyMismatch(AllocationError):
    code = "ALLOCATION_PARTY_MISMATCH"


class AllocationVoucherNotPosted(AllocationError):
    code = "ALLOCATION_VOUCHER_NOT_POSTED"


class AllocationDuplicate(AllocationError):
    code = "ALLOCATION_DUPLICATE"


# ----- DTOs ---------------------------------------------------------------

@dataclass(frozen=True)
class AllocationInput:
    bill_reference_id: int
    amount: Decimal


@dataclass(frozen=True)
class AllocateOutput:
    allocating_voucher_id: int
    allocation_ids: list[int]
    affected_bill_ids: list[int]


# ----- Voucher-type -> allowed bill side map ------------------------------

_VOUCHER_SIDE_MAP = {
    "RECEIPT":     "RECEIVABLE",
    "CREDIT_NOTE": "RECEIVABLE",  # Sales return reduces what customer owes
    "PAYMENT":     "PAYABLE",
    "DEBIT_NOTE":  "PAYABLE",     # Purchase return reduces what we owe
}


# ----- Public API ---------------------------------------------------------

def allocate(
    allocating_voucher_id: int,
    allocations: list[AllocationInput],
) -> AllocateOutput:
    """Allocate a posted voucher to one or more bills. Atomic."""
    if not allocations:
        raise AllocationError("At least one allocation is required.")

    with tx() as conn:
        return _allocate_inline(conn, allocating_voucher_id, allocations)


def _allocate_inline(
    conn,
    allocating_voucher_id: int,
    allocations: list[AllocationInput],
) -> AllocateOutput:
    """Same as allocate() but joins an existing transaction.

    Used by the voucher-creation endpoints to bundle allocation into
    the same tx as the post.
    """
    voucher_repo = VoucherRepository(conn)
    bill_repo = BillReferenceRepository(conn)
    alloc_repo = AllocationRepository(conn)

    # 1. Fetch the allocating voucher. Must be POSTED.
    voucher = voucher_repo.get_header(allocating_voucher_id)
    if voucher is None:
        raise AllocationVoucherNotPosted(
            "Allocating voucher not found in active organization.",
            voucher_id=allocating_voucher_id,
        )
    if voucher["status"] != "POSTED":
        raise AllocationVoucherNotPosted(
            f"Allocating voucher is {voucher['status']}, must be POSTED.",
            voucher_id=allocating_voucher_id,
            status=voucher["status"],
        )
    vtype = voucher["voucher_type"]
    if vtype not in _VOUCHER_SIDE_MAP:
        raise AllocationError(
            f"Voucher type {vtype} cannot allocate to bills.",
            voucher_type=vtype,
        )
    expected_side = _VOUCHER_SIDE_MAP[vtype]
    voucher_party_ledger = voucher["party_ledger_id"]
    if voucher_party_ledger is None:
        raise AllocationError(
            "Allocating voucher has no party_ledger_id set.",
            voucher_id=allocating_voucher_id,
        )

    # 2. Lock + fetch every bill. Validate side + party + amount <= outstanding.
    allocation_ids: list[int] = []
    affected_bill_ids: list[int] = []
    for a in allocations:
        bill = bill_repo.get_by_id(a.bill_reference_id, for_update=True)
        if bill is None:
            raise AllocationError(
                "bill_reference not found in active organization.",
                bill_reference_id=a.bill_reference_id,
            )
        if bill["side"] != expected_side:
            raise AllocationSideMismatch(
                f"{vtype} can only allocate to {expected_side} bills; "
                f"this bill is {bill['side']}.",
                bill_reference_id=a.bill_reference_id,
                expected_side=expected_side,
                actual_side=bill["side"],
            )
        if int(bill["party_ledger_id"]) != int(voucher_party_ledger):
            raise AllocationPartyMismatch(
                "Voucher and bill must reference the same party ledger.",
                bill_reference_id=a.bill_reference_id,
                voucher_party_ledger_id=int(voucher_party_ledger),
                bill_party_ledger_id=int(bill["party_ledger_id"]),
            )
        if Decimal(a.amount) > Decimal(bill["outstanding_amount"]):
            raise AllocationExceedsBill(
                f"Allocation {a.amount} exceeds bill outstanding "
                f"{bill['outstanding_amount']}.",
                bill_reference_id=a.bill_reference_id,
                requested=str(a.amount),
                outstanding=str(bill["outstanding_amount"]),
            )

    # 3. Voucher-level cap: voucher.total_amount is already SUM Dr = SUM Cr,
    #    and for Receipt/Payment vouchers that cap equals the party ledger
    #    exposure. Combined with existing allocations on the same voucher,
    #    the new sum must not exceed voucher.total_amount.
    already = alloc_repo.sum_active_for_voucher(allocating_voucher_id)
    new_sum = sum((Decimal(a.amount) for a in allocations), Decimal("0"))
    if (already + new_sum) > Decimal(voucher["total_amount"]):
        raise AllocationExceedsVoucher(
            f"Requested allocations ({new_sum}) + existing ({already}) "
            f"exceed voucher total ({voucher['total_amount']}).",
            voucher_id=allocating_voucher_id,
            existing_allocations=str(already),
            new_allocations=str(new_sum),
            voucher_total=str(voucher["total_amount"]),
        )

    # 4. Insert allocation rows.
    for a in allocations:
        try:
            alloc_id = alloc_repo.insert(
                allocating_voucher_id=allocating_voucher_id,
                bill_reference_id=a.bill_reference_id,
                amount=a.amount,
            )
        except Exception as e:
            msg = str(e).lower()
            if "duplicate" in msg and "uq_alloc_voucher_bill" in msg:
                raise AllocationDuplicate(
                    "This voucher is already allocated to this bill. "
                    "Cancel the existing allocation and re-allocate.",
                    voucher_id=allocating_voucher_id,
                    bill_reference_id=a.bill_reference_id,
                ) from e
            raise
        allocation_ids.append(alloc_id)
        affected_bill_ids.append(a.bill_reference_id)

    # 5. Recompute outstanding on every affected bill.
    for bid in affected_bill_ids:
        bill_repo.recompute_outstanding(bid)

    return AllocateOutput(
        allocating_voucher_id=allocating_voucher_id,
        allocation_ids=allocation_ids,
        affected_bill_ids=affected_bill_ids,
    )


# ----- Bootstrap a bill_reference directly -------------------------------

def create_opening_bill(
    *,
    party_ledger_id: int,
    bill_no: str,
    bill_date,
    original_amount: Decimal,
    side: str,
    due_date=None,
    notes: Optional[str] = None,
) -> int:
    """Seed an opening-balance bill without a source invoice voucher.

    Used to migrate outstanding AR/AP from Busy into the new system
    before Phase E ships real sales/purchase invoices.
    """
    with tx() as conn:
        bill_repo = BillReferenceRepository(conn)
        return bill_repo.insert(
            party_ledger_id=party_ledger_id,
            bill_no=bill_no,
            bill_date=bill_date,
            original_amount=original_amount,
            side=side,
            source_voucher_id=None,
            due_date=due_date,
            notes=notes or "Opening balance (pre-Phase E)",
        )
