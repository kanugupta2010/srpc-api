"""Tests for core.services.settlement_service."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from core.db import tx
from core.services.posting_service import (
    VoucherInput,
    VoucherLineInput,
    post_voucher,
)
from core.services.settlement_service import (
    AllocationExceedsBill,
    AllocationExceedsVoucher,
    AllocationPartyMismatch,
    AllocationSideMismatch,
    AllocationVoucherNotPosted,
    AllocationDuplicate,
    AllocationInput,
    allocate,
    create_opening_bill,
)
from tests.conftest import D, TEST_ORG_ID


def _seed_customer_ledger(conn, name="ABC Traders") -> int:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id FROM ledgers "
            "WHERE organization_id = %s AND name = %s",
            (TEST_ORG_ID, name),
        )
        row = cur.fetchone()
        return int(row["id"]) if row else 0
    finally:
        cur.close()


def test_allocate_full_bill(bound, coa):
    """Receipt of ₹1000 fully allocated against one ₹1000 bill."""
    customer_id = coa["ABC Traders"]

    # Bootstrap an opening bill.
    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="OPEN-001",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )

    # Post a Receipt voucher of ₹1000.
    receipt = post_voucher(VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=date(2025, 5, 10),
        party_ledger_id=customer_id,
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("1000"),
                             line_narration="Receipt"),
            VoucherLineInput(customer_id,    "Cr", D("1000"),
                             line_narration="Against OPEN-001"),
        ],
    ))

    # Allocate.
    result = allocate(
        receipt.voucher_id,
        [AllocationInput(bill_reference_id=bill_id, amount=D("1000"))],
    )
    assert result.affected_bill_ids == [bill_id]

    # Bill should be CLEARED.
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
    assert row["outstanding_amount"] == D("0.00")
    assert row["status"] == "CLEARED"


def test_allocate_partial_bill(bound, coa):
    """Receipt of ₹600 against a ₹1000 bill leaves ₹400 outstanding, PARTIAL."""
    customer_id = coa["ABC Traders"]
    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="OPEN-002",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )
    receipt = post_voucher(VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=date(2025, 5, 10),
        party_ledger_id=customer_id,
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("600")),
            VoucherLineInput(customer_id,    "Cr", D("600")),
        ],
    ))
    allocate(
        receipt.voucher_id,
        [AllocationInput(bill_reference_id=bill_id, amount=D("600"))],
    )
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
    assert row["outstanding_amount"] == D("400.00")
    assert row["status"] == "PARTIAL"


def test_allocate_across_two_bills(bound, coa):
    """Receipt of ₹1500 pays Bill A (₹1000) fully and Bill B (₹2000) partially."""
    customer_id = coa["ABC Traders"]
    a = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="A",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )
    b = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="B",
        bill_date=date(2025, 4, 2),
        original_amount=D("2000"),
        side="RECEIVABLE",
    )
    receipt = post_voucher(VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=date(2025, 5, 10),
        party_ledger_id=customer_id,
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("1500")),
            VoucherLineInput(customer_id,    "Cr", D("1500")),
        ],
    ))
    allocate(
        receipt.voucher_id,
        [
            AllocationInput(bill_reference_id=a, amount=D("1000")),
            AllocationInput(bill_reference_id=b, amount=D("500")),
        ],
    )
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT id, outstanding_amount, status "
                "FROM bill_references "
                "WHERE organization_id = %s AND id IN (%s, %s)",
                (TEST_ORG_ID, a, b),
            )
            by_id = {int(r["id"]): r for r in cur.fetchall()}
        finally:
            cur.close()
    assert by_id[a]["status"] == "CLEARED"
    assert by_id[a]["outstanding_amount"] == D("0.00")
    assert by_id[b]["status"] == "PARTIAL"
    assert by_id[b]["outstanding_amount"] == D("1500.00")


def test_allocation_exceeds_bill_rejected(bound, coa):
    customer_id = coa["ABC Traders"]
    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="SMALL",
        bill_date=date(2025, 4, 1),
        original_amount=D("500"),
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
    with pytest.raises(AllocationExceedsBill):
        allocate(
            receipt.voucher_id,
            [AllocationInput(bill_reference_id=bill_id, amount=D("600"))],
        )


def test_allocation_exceeds_voucher_rejected(bound, coa):
    customer_id = coa["ABC Traders"]
    a = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="X",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )
    b = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="Y",
        bill_date=date(2025, 4, 2),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )
    receipt = post_voucher(VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=date(2025, 5, 10),
        party_ledger_id=customer_id,
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("1500")),
            VoucherLineInput(customer_id,    "Cr", D("1500")),
        ],
    ))
    with pytest.raises(AllocationExceedsVoucher):
        allocate(
            receipt.voucher_id,
            [
                AllocationInput(bill_reference_id=a, amount=D("1000")),
                AllocationInput(bill_reference_id=b, amount=D("1000")),
            ],
        )


def test_allocation_side_mismatch_rejected(bound, coa):
    """Receipt voucher cannot allocate to PAYABLE bill."""
    customer_id = coa["ABC Traders"]
    # Create a PAYABLE bill — atypical but valid for the test.
    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="PAY-1",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="PAYABLE",
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
    with pytest.raises(AllocationSideMismatch):
        allocate(
            receipt.voucher_id,
            [AllocationInput(bill_reference_id=bill_id, amount=D("1000"))],
        )


def test_allocation_party_mismatch_rejected(bound, coa):
    """Allocating across parties is rejected."""
    customer_id = coa["ABC Traders"]
    # Create a bill for a DIFFERENT party ledger (Sales A/c is not a party
    # ledger, but for testing the mismatch check we only need two different
    # ledger_ids on bill vs voucher).
    other_ledger = coa["Sales A/c"]
    bill_id = create_opening_bill(
        party_ledger_id=other_ledger,
        bill_no="CROSS",
        bill_date=date(2025, 4, 1),
        original_amount=D("1000"),
        side="RECEIVABLE",
    )
    receipt = post_voucher(VoucherInput(
        voucher_type="RECEIPT",
        voucher_date=date(2025, 5, 10),
        party_ledger_id=customer_id,  # voucher is for ABC Traders
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("1000")),
            VoucherLineInput(customer_id,    "Cr", D("1000")),
        ],
    ))
    with pytest.raises(AllocationPartyMismatch):
        allocate(
            receipt.voucher_id,
            [AllocationInput(bill_reference_id=bill_id, amount=D("1000"))],
        )


def test_double_allocation_same_bill_rejected(bound, coa):
    """Cannot insert two allocations for (voucher, bill). Must cancel + repost."""
    customer_id = coa["ABC Traders"]
    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="DUP",
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
        [AllocationInput(bill_reference_id=bill_id, amount=D("400"))],
    )
    with pytest.raises(AllocationDuplicate):
        allocate(
            receipt.voucher_id,
            [AllocationInput(bill_reference_id=bill_id, amount=D("600"))],
        )


def test_allocation_on_non_posted_voucher_rejected(bound, coa):
    """Allocating against a non-existent voucher is rejected."""
    customer_id = coa["ABC Traders"]
    bill_id = create_opening_bill(
        party_ledger_id=customer_id,
        bill_no="NO-VOUCHER",
        bill_date=date(2025, 4, 1),
        original_amount=D("100"),
        side="RECEIVABLE",
    )
    with pytest.raises(AllocationVoucherNotPosted):
        allocate(
            9_999_999,
            [AllocationInput(bill_reference_id=bill_id, amount=D("50"))],
        )
