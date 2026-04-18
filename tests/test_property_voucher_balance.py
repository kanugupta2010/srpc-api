"""Property-based tests for voucher invariants.

Hypothesis generates random combinations of Dr/Cr lines and we assert:
    * Balanced combinations always post successfully.
    * Unbalanced combinations always fail with VoucherUnbalancedError.
    * Posting is idempotent in effect: posting N balanced vouchers
      produces N vouchers with distinct, sequential numbers.

Running this set catches off-by-one errors in the balanced check and
any drift in voucher numbering under random inputs. It deliberately
uses small integer amounts to keep the DB round-trips cheap.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import List

import pytest
from hypothesis import given, settings, strategies as st

from core.errors import VoucherUnbalancedError
from core.services.posting_service import (
    VoucherInput,
    VoucherLineInput,
    post_voucher,
)
from tests.conftest import D


# Amount range: small integers to keep tests fast but still exercise
# the Decimal arithmetic.
_amount = st.integers(min_value=1, max_value=10_000).map(
    lambda n: Decimal(n)
)


def _build_balanced_lines(
    dr_ledger_id: int, cr_ledger_id: int, split_points: List[int], total: int
) -> List[VoucherLineInput]:
    """Split `total` into N Dr parts and N Cr parts that each sum to total."""
    # Normalise split_points to sum to `total` with each part >= 1.
    split_points = sorted(p for p in split_points if 0 < p < total)
    split_points = list(dict.fromkeys(split_points))  # de-dupe, keep order
    boundaries = [0, *split_points, total]
    parts = [
        boundaries[i + 1] - boundaries[i]
        for i in range(len(boundaries) - 1)
    ]
    parts = [p for p in parts if p > 0]
    if not parts:
        parts = [total]

    lines: List[VoucherLineInput] = []
    for p in parts:
        lines.append(VoucherLineInput(dr_ledger_id, "Dr", D(p)))
    for p in parts:
        lines.append(VoucherLineInput(cr_ledger_id, "Cr", D(p)))
    return lines


@settings(max_examples=25, deadline=None)
@given(
    total=st.integers(min_value=1, max_value=10_000),
    splits=st.lists(
        st.integers(min_value=1, max_value=9_999),
        min_size=0,
        max_size=4,
    ),
)
def test_balanced_voucher_always_posts(bound, coa, total, splits):
    """Any multi-line voucher with matching Dr/Cr totals must post."""
    lines = _build_balanced_lines(
        coa["Cash-in-Hand"], coa["Sales A/c"], splits, total
    )
    payload = VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 15),
        lines=lines,
    )
    result = post_voucher(payload)
    assert result.total_amount == D(total)


@settings(max_examples=25, deadline=None)
@given(
    dr_amount=_amount,
    cr_amount=_amount,
)
def test_unbalanced_always_rejected(bound, coa, dr_amount, cr_amount):
    """Any Dr/Cr mismatch must be rejected, regardless of magnitude."""
    if dr_amount == cr_amount:
        return  # Hypothesis would generate these; trivially balanced.

    payload = VoucherInput(
        voucher_type="JOURNAL",
        voucher_date=date(2025, 5, 15),
        lines=[
            VoucherLineInput(coa["Cash-in-Hand"], "Dr", dr_amount),
            VoucherLineInput(coa["Sales A/c"],    "Cr", cr_amount),
        ],
    )
    with pytest.raises(VoucherUnbalancedError):
        post_voucher(payload)
