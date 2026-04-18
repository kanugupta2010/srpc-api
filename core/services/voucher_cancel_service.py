"""Voucher cancel service — extends Phase B's cancel_voucher() with
allocation reversal and audit logging.

When a Receipt or Payment voucher is cancelled, any allocations it
made against bill_references must also be reversed (not deleted —
marked is_reversed=1 so the audit trail is preserved). The bill's
outstanding_amount is recomputed in the same transaction.

This cannot live inside core/services/posting_service.cancel_voucher()
because posting_service doesn't know about the settlement context.
Instead, posting_service.cancel_voucher() remains the low-level
primitive and THIS service wraps it with the higher-level "cancel +
reverse allocations + audit" flow used by the API.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..db import tx
from ..repos.allocations import AllocationRepository
from ..repos.bill_references import BillReferenceRepository
from ..repos.vouchers import VoucherRepository
from ..tenancy import get_active_org_id
from .posting_service import (
    PostedVoucher,
    VoucherAlreadyCancelledError,
    VoucherInput,
    VoucherLineInput,
    VoucherNotPostedError,
    VoucherUnbalancedError,
    _post_voucher_inline,
)


@dataclass(frozen=True)
class CancelResult:
    cancelled_voucher_id: int
    reversing_voucher_id: int
    reversing_voucher_number: str
    reversed_allocation_bill_ids: list[int]


def cancel_voucher_with_allocations(
    voucher_id: int,
    *,
    cancelled_by: Optional[str] = None,
    reason: Optional[str] = None,
) -> CancelResult:
    """Cancel voucher + reverse its allocations + log to audit_log."""
    with tx() as conn:
        voucher_repo = VoucherRepository(conn)
        alloc_repo = AllocationRepository(conn)
        bill_repo = BillReferenceRepository(conn)

        original = voucher_repo.get_header(voucher_id)
        if original is None:
            raise VoucherNotPostedError(
                "Voucher does not exist in active organization.",
                voucher_id=voucher_id,
            )
        if original["status"] == "CANCELLED":
            raise VoucherAlreadyCancelledError(
                "Voucher is already cancelled.",
                voucher_id=voucher_id,
            )
        if original["status"] != "POSTED":
            raise VoucherNotPostedError(
                f"Voucher status is {original['status']}; cannot cancel.",
                voucher_id=voucher_id,
                status=original["status"],
            )

        # 1. Reverse any allocations this voucher made.
        affected_bill_ids = alloc_repo.mark_reversed_for_voucher(voucher_id)
        for bid in affected_bill_ids:
            bill_repo.recompute_outstanding(bid)

        # 2. Flip original to CANCELLED.
        voucher_repo.mark_cancelled(voucher_id)

        # 3. Post the reversing voucher in the same transaction.
        original_lines = voucher_repo.get_lines(voucher_id)
        if not original_lines:
            raise VoucherUnbalancedError(
                "Original voucher has no lines; cannot reverse.",
                voucher_id=voucher_id,
            )

        flipped = [
            VoucherLineInput(
                ledger_id=int(ln["ledger_id"]),
                dr_cr=("Cr" if ln["dr_cr"] == "Dr" else "Dr"),
                amount=ln["amount"],
                cost_center_id=(
                    int(ln["cost_center_id"])
                    if ln["cost_center_id"] is not None
                    else None
                ),
                line_narration=f"Reversal: {ln['line_narration'] or ''}".strip(),
            )
            for ln in original_lines
        ]

        reversal_narration = (
            f"Cancellation of {original['voucher_type']} "
            f"{original['voucher_number']}"
            + (f" — {reason}" if reason else "")
        )

        reversing = _post_voucher_inline(
            conn,
            VoucherInput(
                voucher_type=original["voucher_type"],
                voucher_date=original["voucher_date"],
                lines=flipped,
                party_ledger_id=(
                    int(original["party_ledger_id"])
                    if original["party_ledger_id"] is not None
                    else None
                ),
                reference_no=original["voucher_number"],
                narration=reversal_narration,
                source_doc_type="REVERSAL_OF",
                source_doc_id=voucher_id,
                created_by=cancelled_by,
            ),
        )

        # 4. Audit.
        _audit(
            conn,
            entity_type="voucher",
            entity_id=voucher_id,
            action="CANCELLED",
            actor=cancelled_by,
            details={
                "reason": reason,
                "reversing_voucher_id": reversing.voucher_id,
                "reversing_voucher_number": reversing.voucher_number,
                "reversed_allocation_bill_ids": affected_bill_ids,
            },
        )

        return CancelResult(
            cancelled_voucher_id=voucher_id,
            reversing_voucher_id=reversing.voucher_id,
            reversing_voucher_number=reversing.voucher_number,
            reversed_allocation_bill_ids=affected_bill_ids,
        )


# ----- Audit log helper (used by several services) ----------------------

def _audit(
    conn,
    *,
    entity_type: str,
    entity_id: Optional[int],
    action: str,
    actor: Optional[str],
    details: Optional[dict] = None,
) -> None:
    import json

    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO audit_log ("
            "    organization_id, entity_type, entity_id, action, "
            "    actor, details"
            ") VALUES ("
            "    %(org_id)s, %(et)s, %(eid)s, %(ac)s, %(act)s, %(d)s"
            ")",
            {
                "org_id": get_active_org_id(),
                "et": entity_type,
                "eid": entity_id,
                "ac": action,
                "act": actor,
                "d": json.dumps(details) if details is not None else None,
            },
        )
    finally:
        cur.close()
