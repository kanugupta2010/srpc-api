"""Tests for core.services.voucher_cancel_service.

Exercises the Phase C cancel flow that extends Phase B's cancellation
with allocation reversal and audit logging.
"""
from __future__ import annotations

from datetime import date

import pytest

from core.db import tx
from core.services.posting_service import (
    VoucherInput,
    VoucherLineInput,
    post_voucher,
)
from core.services.settlement_service import (
    AllocationInput,
    allocate,
    create_opening_bill,
)
from core.services.voucher_cancel_service import (
    VoucherAlreadyCancelledError,
    VoucherNotPostedError,
    cancel_voucher_with_allocations,
)
from tests.conftest import D, TEST_ORG_ID


def test_cancel_simple_voucher_no_allocations(bound, coa):
    """Cancellation of a plain journal posts a reversing entry."""
    original = post_voucher(VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("500")),
            VoucherLineInput(coa["Sales A/c"],    "Cr", D("500")),
        ],
    ))

    result = cancel_voucher_with_allocations(
        original.voucher_id,
        cancelled_by="admin",
        reason="entered in error",
    )
    assert result.cancelled_voucher_id == original.voucher_id
    assert result.reversing_voucher_id != original.voucher_id
    assert result.reversed_allocation_bill_ids == []

    # Original must be CANCELLED; reversing must be POSTED with
    # source_doc_type='REVERSAL_OF'.
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT status FROM vouchers "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, original.voucher_id),
            )
            orig = cur.fetchone()
            cur.execute(
                "SELECT status, source_doc_type, source_doc_id "
                "FROM vouchers "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, result.reversing_voucher_id),
            )
            rev = cur.fetchone()
        finally:
            cur.close()

    assert orig["status"] == "CANCELLED"
    assert rev["status"] == "POSTED"
    assert rev["source_doc_type"] == "REVERSAL_OF"
    assert rev["source_doc_id"] == original.voucher_id


def test_cancel_receipt_reverses_allocations(bound, coa):
    """Cancelling an allocated receipt reopens the bill it had cleared."""
    customer_id = coa["ABC Traders"]

    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="BILL-CANCEL-001",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )

    receipt = post_voucher(VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=date(2025, 5, 10),
        party_ledger_id=customer_id,
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("1000")),
            VoucherLineInput(customer_id,    "Cr", D("1000")),
        ],
    ))

    allocate(
        receipt.voucher_id,
        [AllocationInput(bill_reference_id=bill_id, amount=D("1000"))],
    )

    # Sanity: bill is CLEARED before cancellation.
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT outstanding_amount, status FROM bill_references "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, bill_id),
            )
            before = cur.fetchone()
        finally:
            cur.close()
    assert before["status"] == "CLEARED"
    assert before["outstanding_amount"] == D("0.00")

    # Cancel the receipt.
    result = cancel_voucher_with_allocations(
        receipt.voucher_id,
        cancelled_by="admin",
        reason="Bounced cheque",
    )
    assert result.reversed_allocation_bill_ids == [bill_id]

    # Bill must be OPEN again with full outstanding.
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT outstanding_amount, status FROM bill_references "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, bill_id),
            )
            after = cur.fetchone()

            # Allocation row must be marked is_reversed=1, not deleted.
            cur.execute(
                "SELECT is_reversed, reversed_at FROM allocations "
                "WHERE organization_id = %s "
                "  AND allocating_voucher_id = %s",
                (TEST_ORG_ID, receipt.voucher_id),
            )
            alloc = cur.fetchone()

            # audit_log entry must exist.
            cur.execute(
                "SELECT entity_type, entity_id, action, actor "
                "FROM audit_log "
                "WHERE organization_id = %s "
                "  AND entity_type = 'voucher' "
                "  AND entity_id = %s "
                "  AND action = 'CANCELLED'",
                (TEST_ORG_ID, receipt.voucher_id),
            )
            audit = cur.fetchone()
        finally:
            cur.close()

    assert after["outstanding_amount"] == D("1000.00")
    assert after["status"] == "OPEN"
    assert alloc["is_reversed"] == 1
    assert alloc["reversed_at"] is not None
    assert audit is not None
    assert audit["actor"] == "admin"


def test_cancel_partial_allocation_restores_partial(bound, coa):
    """Cancelling a receipt that only partly paid a bill restores original."""
    customer_id = coa["ABC Traders"]

    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="BILL-PARTIAL-001",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )

    receipt = post_voucher(VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=date(2025, 5, 10),
        party_ledger_id=customer_id,
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("400")),
            VoucherLineInput(customer_id,    "Cr", D("400")),
        ],
    ))
    allocate(
        receipt.voucher_id,
        [AllocationInput(bill_reference_id=bill_id, amount=D("400"))],
    )

    cancel_voucher_with_allocations(receipt.voucher_id, cancelled_by="admin")

    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT outstanding_amount, status FROM bill_references "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, bill_id),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    assert row["outstanding_amount"] == D("1000.00")
    assert row["status"] == "OPEN"


def test_double_cancel_rejected(bound, coa):
    original = post_voucher(VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("10")),
            VoucherLineInput(coa["Sales A/c"],    "Cr", D("10")),
        ],
    ))
    cancel_voucher_with_allocations(original.voucher_id, cancelled_by="admin")
    with pytest.raises(VoucherAlreadyCancelledError):
        cancel_voucher_with_allocations(original.voucher_id, cancelled_by="admin")


def test_cancel_nonexistent_voucher(bound, coa):
    with pytest.raises(VoucherNotPostedError):
        cancel_voucher_with_allocations(9_999_999, cancelled_by="admin")
