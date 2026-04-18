"""Behavioural tests for core.services.posting_service."""
from __future__ import annotations

from datetime import date

import pytest

from core.errors import (
    CrossOrgReferenceError,
    VoucherAlreadyCancelledError,
    VoucherPeriodLockedError,
    VoucherTypeRuleViolation,
    VoucherUnbalancedError,
)
from core.services.posting_service import (
    VoucherInput,
    VoucherLineInput,
    cancel_voucher,
    post_voucher,
)
from core.db import tx
from tests.conftest import D, TEST_ORG_ID


def test_simple_journal_posts_successfully(bound, coa):
    """Smoke test — a balanced 2-line journal posts."""
    payload = VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        narration="Test opening entry",
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("1000")),
            VoucherLineInput(coa["Sales A/c"],    "Cr", D("1000")),
        ],
    )
    result = post_voucher(payload)
    assert result.voucher_number == "JV0001"
    assert result.total_amount == D("1000")
    assert len(result.line_ids) == 2


def test_unbalanced_voucher_is_rejected(bound, coa):
    payload = VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("1000")),
            VoucherLineInput(coa["Sales A/c"],    "Cr", D("999.99")),
        ],
    )
    with pytest.raises(VoucherUnbalancedError) as ei:
        post_voucher(payload)
    assert ei.value.details["difference"] == "0.01"


def test_single_sided_voucher_is_rejected(bound, coa):
    payload = VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("500")),
            VoucherLineInput(coa["Sales A/c"],    "Dr", D("500")),
        ],
    )
    with pytest.raises(VoucherUnbalancedError):
        post_voucher(payload)


def test_contra_between_cash_and_bank_allowed(bound, coa):
    payload = VoucherInput(
        voucher_type="CONTRA",
        voucher_date=date(2025, 5, 10),
        narration="Cash deposit",
        lines=[
            VoucherLineInput(coa["HDFC Bank"],    "Dr", D("5000")),
            VoucherLineInput(coa["Cash-in-Hand"], "Cr", D("5000")),
        ],
    )
    result = post_voucher(payload)
    assert result.voucher_number == "CV0001"


def test_contra_with_non_cash_bank_ledger_rejected(bound, coa):
    """Contra touching a Sales ledger must be rejected."""
    payload = VoucherInput(
        voucher_type="CONTRA",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["HDFC Bank"], "Dr", D("100")),
            VoucherLineInput(coa["Sales A/c"], "Cr", D("100")),
        ],
    )
    with pytest.raises(VoucherTypeRuleViolation):
        post_voucher(payload)


def test_posting_to_locked_period_is_rejected(bound, coa):
    # Lock the test FY
    with tx() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE financial_years SET is_locked = 1 "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, coa["__fy_id__"]),
            )
        finally:
            cur.close()

    try:
        payload = VoucherInput(
            voucher_type="JOURNAL",
            voucher_date=date(2025, 5, 10),
            lines=[
                VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("10")),
                VoucherLineInput(coa["Sales A/c"],    "Cr", D("10")),
            ],
        )
        with pytest.raises(VoucherPeriodLockedError):
            post_voucher(payload)
    finally:
        # Unlock so teardown can delete rows.
        with tx() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    "UPDATE financial_years SET is_locked = 0 "
                    "WHERE organization_id = %s AND id = %s",
                    (TEST_ORG_ID, coa["__fy_id__"]),
                )
            finally:
                cur.close()


def test_cross_org_ledger_reference_is_rejected(bound, coa):
    """Referencing a ledger from org 1 while bound to org 9999 must fail."""
    # Find any ledger id in SRPC org (1). If the DB has no ledger for org 1
    # yet (fresh env), this test is trivially satisfied — skip gracefully.
    with tx() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM ledgers WHERE organization_id = 1 LIMIT 1"
            )
            row = cur.fetchone()
        finally:
            cur.close()
    if not row:
        pytest.skip("No ledger in org 1 to test cross-org reference.")

    other_org_ledger_id = int(row[0])
    payload = VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(other_org_ledger_id,  "Dr", D("1")),
            VoucherLineInput(coa["Sales A/c"],     "Cr", D("1")),
        ],
    )
    with pytest.raises(CrossOrgReferenceError):
        post_voucher(payload)


def test_cancel_posts_reversing_voucher(bound, coa):
    """Cancellation marks original CANCELLED and posts reversing JV."""
    original = post_voucher(VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("750")),
            VoucherLineInput(coa["Sales A/c"],    "Cr", D("750")),
        ],
    ))

    reversal = cancel_voucher(original.voucher_id, reason="Entered in error")
    assert reversal.voucher_id != original.voucher_id
    assert reversal.voucher_type == "JOURNAL"
    assert reversal.total_amount == D("750")

    # Verify DB state.
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT status, cancelled_at FROM vouchers "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, original.voucher_id),
            )
            orig_row = cur.fetchone()
            cur.execute(
                "SELECT source_doc_type, source_doc_id, status "
                "FROM vouchers "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, reversal.voucher_id),
            )
            rev_row = cur.fetchone()

            # Sum Dr/Cr per ledger across both vouchers — must net to zero.
            cur.execute(
                "SELECT ledger_id, dr_cr, amount FROM voucher_lines "
                "WHERE organization_id = %s "
                "  AND voucher_id IN (%s, %s)",
                (TEST_ORG_ID, original.voucher_id, reversal.voucher_id),
            )
            all_lines = cur.fetchall()
        finally:
            cur.close()

    assert orig_row["status"] == "CANCELLED"
    assert orig_row["cancelled_at"] is not None
    assert rev_row["status"] == "POSTED"
    assert rev_row["source_doc_type"] == "REVERSAL_OF"
    assert rev_row["source_doc_id"] == original.voucher_id

    # Per-ledger net must be zero.
    from collections import defaultdict
    net = defaultdict(lambda: D("0"))
    for ln in all_lines:
        sign = D("1") if ln["dr_cr"] == "Dr" else D("-1")
        net[ln["ledger_id"]] += sign * D(ln["amount"])
    for lid, amt in net.items():
        assert amt == D("0"), f"Ledger {lid} not zeroed: {amt}"


def test_double_cancel_is_rejected(bound, coa):
    original = post_voucher(VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("10")),
            VoucherLineInput(coa["Sales A/c"],    "Cr", D("10")),
        ],
    ))
    cancel_voucher(original.voucher_id)
    with pytest.raises(VoucherAlreadyCancelledError):
        cancel_voucher(original.voucher_id)


def test_voucher_numbering_is_sequential_per_type(bound, coa):
    """Separate types have separate series; numbers don't collide."""
    r1 = post_voucher(VoucherInput(
        voucher_type="PAYMENT",
        voucher_date=date(2025, 5, 10),
        lines=[
            VoucherLineInput(coa["Sales A/c"],    "Dr", D("100")),
            VoucherLineInput(coa["Cash-in-Hand"], "Cr", D("100")),
        ],
    ))
    r2 = post_voucher(VoucherInput(
        voucher_type="PAYMENT",
        voucher_date=date(2025, 5, 11),
        lines=[
            VoucherLineInput(coa["Sales A/c"],    "Dr", D("200")),
            VoucherLineInput(coa["Cash-in-Hand"], "Cr", D("200")),
        ],
    ))
    j1 = post_voucher(VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 11),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", D("50")),
            VoucherLineInput(coa["Sales A/c"],    "Cr", D("50")),
        ],
    ))

    assert r1.voucher_number == "PV0001"
    assert r2.voucher_number == "PV0002"
    assert j1.voucher_number == "JV0001"
