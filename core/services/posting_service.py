"""Voucher posting service — the single authoritative path for creating
and cancelling vouchers in the accounting core.

Every voucher in the system goes through `post_voucher()`. That function
is the only place that:

    * Validates the balanced-debits invariant (Σ Dr == Σ Cr to the paisa)
    * Confirms all referenced ledgers belong to the active organization
    * Confirms the posting date falls in an OPEN financial year
    * Enforces voucher-type-specific rules (e.g. Contra: cash/bank only)
    * Locks the voucher_series row (SELECT ... FOR UPDATE) and issues
      the next voucher_number atomically
    * Writes the header and lines inside a single DB transaction

If ANY check fails, nothing is written. A domain exception from
core.errors is raised; the caller (router or background job) is
expected to let it bubble up and the HTTP layer translates it into
the { code, message, details } envelope from CloudAccountingDesign
§17.3.

Cancellation does NOT mutate posted rows. `cancel_voucher()` marks
the original CANCELLED (the only state change allowed post-posting)
and inserts a new voucher of the same type with Dr/Cr flipped,
`source_doc_type='REVERSAL_OF'`, `source_doc_id=<original.id>`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional

from ..db import tx
from ..errors import (
    CrossOrgReferenceError,
    VoucherAlreadyCancelledError,
    VoucherDuplicateNumberError,
    VoucherNotPostedError,
    VoucherTypeRuleViolation,
    VoucherUnbalancedError,
)
from ..repos.ledgers import LedgerRepository
from ..repos.voucher_series import (
    FinancialYearRepository,
    VoucherSeriesRepository,
)
from ..repos.vouchers import VoucherRepository
from ..tenancy import get_active_org_id

# --- Constants ------------------------------------------------------------

# Voucher type -> allowed ledger account-group *natures*. Used by
# `_validate_type_rules`. Keeping this as a dict rather than an enum
# map because the rules are declarative and shouldn't need code.
#
# * PAYMENT: Cr must hit Cash/Bank (ASSET); Dr is anything.
# * RECEIPT: Dr must hit Cash/Bank (ASSET); Cr is anything.
# * CONTRA:  every line must hit Cash-in-Hand or Bank Accounts.
# * Others:  no structural rule here — balanced-debits is sufficient.
#
# The rule expressed as reserved group names (from the seed script)
# rather than nature so a future "Cash-in-Hand under a different
# parent" wouldn't accidentally qualify.
_CASH_BANK_RESERVED_GROUPS = {"Cash-in-Hand", "Bank Accounts"}

# Allowed voucher types — mirrors the enum in the vouchers table.
ALLOWED_VOUCHER_TYPES = frozenset(
    {
        "PAYMENT",
        "RECEIPT",
        "JOURNAL",
        "CONTRA",
        "SALES",
        "PURCHASE",
        "CREDIT_NOTE",
        "DEBIT_NOTE",
        "STOCK_JOURNAL",
    }
)


# --- DTOs -----------------------------------------------------------------

@dataclass(frozen=True)
class VoucherLineInput:
    """One Dr or Cr line to be written."""

    ledger_id: int
    dr_cr: str  # 'Dr' or 'Cr'
    amount: Decimal
    cost_center_id: Optional[int] = None
    line_narration: Optional[str] = None

    def __post_init__(self) -> None:
        if self.dr_cr not in ("Dr", "Cr"):
            raise ValueError(f"dr_cr must be 'Dr' or 'Cr', got {self.dr_cr!r}")
        if not isinstance(self.amount, Decimal):
            raise TypeError(
                f"amount must be Decimal, got {type(self.amount).__name__}"
            )
        if self.amount <= Decimal("0"):
            raise ValueError(f"amount must be > 0, got {self.amount}")


@dataclass(frozen=True)
class VoucherInput:
    """Everything needed to post a single voucher."""

    voucher_type: str
    voucher_date: date
    lines: list[VoucherLineInput]
    party_ledger_id: Optional[int] = None
    reference_no: Optional[str] = None
    narration: Optional[str] = None
    source_doc_type: Optional[str] = None
    source_doc_id: Optional[int] = None
    created_by: Optional[str] = None
    # Optional: if the caller wants a specific series (e.g. counter-1
    # for a POS), it can be supplied. Otherwise the 'Default' series
    # for (org, type, fy) is used.
    voucher_series_id: Optional[int] = None


@dataclass(frozen=True)
class PostedVoucher:
    """Summary returned after a successful post."""

    voucher_id: int
    voucher_number: str
    voucher_type: str
    total_amount: Decimal
    line_ids: list[int] = field(default_factory=list)


# --- Public API -----------------------------------------------------------

def post_voucher(payload: VoucherInput) -> PostedVoucher:
    """Post a voucher. Atomic; raises a DomainError on any rule violation.

    Call from inside a FastAPI endpoint (or background task) that has
    already bound the active organization_id via `bind_org(...)` or the
    FastAPI dependency.
    """
    _validate_input_shape(payload)

    # Open the transaction. Everything below either all succeeds or all
    # rolls back. MySQL InnoDB default isolation (REPEATABLE READ) is
    # fine here — combined with SELECT ... FOR UPDATE on the series row,
    # it prevents duplicate voucher numbers under concurrency.
    with tx() as conn:
        ledger_repo = LedgerRepository(conn)
        fy_repo = FinancialYearRepository(conn)
        series_repo = VoucherSeriesRepository(conn)
        voucher_repo = VoucherRepository(conn)

        # 1. Resolve financial year (also checks is_locked).
        fy = fy_repo.get_for_date(payload.voucher_date)

        # 2. Resolve voucher series (default unless caller specified one).
        if payload.voucher_series_id is None:
            series = series_repo.get_default_for(
                payload.voucher_type, fy["id"]
            )
            if series is None:
                from ..errors import VoucherSeriesNotFoundError

                raise VoucherSeriesNotFoundError(
                    f"No 'Default' voucher series configured for "
                    f"{payload.voucher_type} in FY {fy['code']}. "
                    "Seed one via scripts/seed_phase_a.py or create "
                    "a series before posting.",
                    voucher_type=payload.voucher_type,
                    fy_code=fy["code"],
                )
            series_id = int(series["id"])
        else:
            series_id = payload.voucher_series_id

        # 3. Cross-org + active-ness checks for every referenced ledger.
        ledger_ids = [ln.ledger_id for ln in payload.lines]
        if payload.party_ledger_id is not None:
            ledger_ids.append(payload.party_ledger_id)
        # De-dupe for the query.
        unique_ledger_ids = list({lid for lid in ledger_ids})
        ledger_repo.assert_all_belong_to_org(unique_ledger_ids)
        ledger_repo.assert_all_active(unique_ledger_ids)

        # 4. Voucher-type-specific structural rules.
        _validate_type_rules(conn, payload)

        # 5. Balanced invariant — Σ Dr == Σ Cr to the paisa.
        total = _assert_balanced(payload.lines)

        # 6. Issue voucher_number under row lock.
        voucher_number = series_repo.issue_next_number(series_id)

        # 7. Insert header. Uniqueness constraint on
        #    (org, type, fy, voucher_number) is the backstop against
        #    any race that slips past the SELECT FOR UPDATE (there
        #    shouldn't be one, but belt + braces).
        try:
            voucher_id = voucher_repo.insert_header(
                voucher_type=payload.voucher_type,
                voucher_series_id=series_id,
                financial_year_id=int(fy["id"]),
                voucher_number=voucher_number,
                voucher_date=payload.voucher_date,
                reference_no=payload.reference_no,
                party_ledger_id=payload.party_ledger_id,
                narration=payload.narration,
                total_amount=total,
                source_doc_type=payload.source_doc_type,
                source_doc_id=payload.source_doc_id,
                created_by=payload.created_by,
            )
        except Exception as e:  # mysql.connector.errors.IntegrityError etc.
            # Surface the dup-key case with the typed domain error.
            msg = str(e).lower()
            if "duplicate" in msg and "uq_voucher_number" in msg:
                raise VoucherDuplicateNumberError(
                    f"Voucher number {voucher_number} already exists for "
                    f"{payload.voucher_type} in FY {fy['code']}.",
                    voucher_number=voucher_number,
                    voucher_type=payload.voucher_type,
                    fy_code=fy["code"],
                ) from e
            raise

        # 8. Insert lines in declared order.
        line_ids: list[int] = []
        for idx, ln in enumerate(payload.lines):
            line_id = voucher_repo.insert_line(
                voucher_id=voucher_id,
                ledger_id=ln.ledger_id,
                dr_cr=ln.dr_cr,
                amount=ln.amount,
                cost_center_id=ln.cost_center_id,
                line_narration=ln.line_narration,
                line_order=idx,
            )
            line_ids.append(line_id)

        return PostedVoucher(
            voucher_id=voucher_id,
            voucher_number=voucher_number,
            voucher_type=payload.voucher_type,
            total_amount=total,
            line_ids=line_ids,
        )


def cancel_voucher(
    voucher_id: int,
    *,
    cancelled_by: Optional[str] = None,
    reason: Optional[str] = None,
) -> PostedVoucher:
    """Cancel a POSTED voucher by inserting a reversing voucher.

    Atomic: either both the original flip-to-CANCELLED and the reversing
    voucher succeed, or neither does. Returns the newly-created reversing
    voucher.
    """
    with tx() as conn:
        voucher_repo = VoucherRepository(conn)

        original = voucher_repo.get_header(voucher_id)
        if original is None:
            raise VoucherNotPostedError(
                "Voucher does not exist in the active organization.",
                voucher_id=voucher_id,
            )
        if original["status"] == "CANCELLED":
            raise VoucherAlreadyCancelledError(
                "Voucher is already cancelled.",
                voucher_id=voucher_id,
            )
        if original["status"] != "POSTED":
            raise VoucherNotPostedError(
                f"Voucher status is {original['status']}; only POSTED "
                "vouchers can be cancelled.",
                voucher_id=voucher_id,
                status=original["status"],
            )

        original_lines = voucher_repo.get_lines(voucher_id)
        if not original_lines:
            # Defensive — shouldn't happen if the post went through
            # post_voucher() but surface it loudly if it ever does.
            raise VoucherUnbalancedError(
                "Original voucher has no lines; cannot construct reversal.",
                voucher_id=voucher_id,
            )

        # Flip Dr <-> Cr on every line.
        flipped = [
            VoucherLineInput(
                ledger_id=int(ln["ledger_id"]),
                dr_cr=("Cr" if ln["dr_cr"] == "Dr" else "Dr"),
                amount=Decimal(ln["amount"]),
                cost_center_id=(
                    int(ln["cost_center_id"])
                    if ln["cost_center_id"] is not None
                    else None
                ),
                line_narration=(
                    f"Reversal of {original['voucher_number']}: "
                    f"{ln['line_narration'] or ''}".strip().rstrip(":")
                ),
            )
            for ln in original_lines
        ]

        reversal_narration = (
            f"Cancellation of {original['voucher_type']} "
            f"{original['voucher_number']}"
            + (f" — {reason}" if reason else "")
        )

        # Mark original cancelled BEFORE writing the reversal so that if
        # mark_cancelled fails (e.g. status raced to CANCELLED), the
        # reversal is never written.
        voucher_repo.mark_cancelled(voucher_id)

        # Post reversal through the normal path — all invariants re-run.
        # voucher_date is today's date, not the original's, because a
        # reversal that lands in the same (now-locked) period would fail.
        # If the caller wants the reversal dated to the original voucher's
        # date, they must ensure that FY is still open.
        reversal_date = original["voucher_date"]  # keep in same period
        reversal = _post_voucher_inline(
            conn,
            VoucherInput(
                voucher_type=original["voucher_type"],
                voucher_date=reversal_date,
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
        return reversal


# --- Internal helpers -----------------------------------------------------

def _validate_input_shape(payload: VoucherInput) -> None:
    """Cheap, pre-DB sanity checks. DB checks come from the invariant."""
    if payload.voucher_type not in ALLOWED_VOUCHER_TYPES:
        raise VoucherTypeRuleViolation(
            f"Unknown voucher_type {payload.voucher_type!r}.",
            voucher_type=payload.voucher_type,
        )
    if len(payload.lines) < 2:
        raise VoucherUnbalancedError(
            "A voucher must have at least 2 lines (one Dr and one Cr).",
            line_count=len(payload.lines),
        )
    has_dr = any(ln.dr_cr == "Dr" for ln in payload.lines)
    has_cr = any(ln.dr_cr == "Cr" for ln in payload.lines)
    if not (has_dr and has_cr):
        raise VoucherUnbalancedError(
            "A voucher must have at least one Dr line and one Cr line.",
            has_dr=has_dr,
            has_cr=has_cr,
        )


def _assert_balanced(lines: list[VoucherLineInput]) -> Decimal:
    """Return total (= Σ Dr = Σ Cr) or raise VoucherUnbalancedError."""
    total_dr = sum((ln.amount for ln in lines if ln.dr_cr == "Dr"), Decimal("0"))
    total_cr = sum((ln.amount for ln in lines if ln.dr_cr == "Cr"), Decimal("0"))
    if total_dr != total_cr:
        raise VoucherUnbalancedError(
            f"Voucher is not balanced: Dr = {total_dr}, Cr = {total_cr}, "
            f"difference = {total_dr - total_cr}.",
            total_dr=str(total_dr),
            total_cr=str(total_cr),
            difference=str(total_dr - total_cr),
        )
    return total_dr


def _validate_type_rules(conn, payload: VoucherInput) -> None:
    """Enforce voucher-type-specific structural rules.

    Currently: Contra vouchers may only touch Cash-in-Hand or Bank Accounts
    ledgers. The spec lists a few more (e.g. Payment must credit cash/bank),
    but the cleanest way to enforce those is to let unbalanced/structural
    business rules live in the specific-voucher services we'll add in
    Phase C (e.g. a PaymentVoucherService that constructs the lines
    internally from a typed payload and therefore can't be misused).
    """
    if payload.voucher_type != "CONTRA":
        return

    ledger_ids = [ln.ledger_id for ln in payload.lines]
    # Find the group names of every ledger in the voucher. Reserved
    # groups are created by the seed script with is_reserved=1 and
    # well-known names like 'Cash-in-Hand' and 'Bank Accounts'.
    placeholders = ", ".join(
        f"%(id_{i})s" for i in range(len(ledger_ids))
    )
    params = {"org_id": get_active_org_id()}
    params.update({f"id_{i}": lid for i, lid in enumerate(ledger_ids)})

    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            f"SELECT l.id AS ledger_id, g.name AS group_name "
            f"FROM ledgers l "
            f"JOIN account_groups g "
            f"  ON g.id = l.group_id AND g.organization_id = l.organization_id "
            f"WHERE l.organization_id = %(org_id)s "
            f"  AND l.id IN ({placeholders})",
            params,
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    offenders = [
        row["ledger_id"]
        for row in rows
        if row["group_name"] not in _CASH_BANK_RESERVED_GROUPS
    ]
    if offenders:
        raise VoucherTypeRuleViolation(
            "Contra vouchers may only touch Cash-in-Hand or Bank Accounts "
            "ledgers.",
            offending_ledger_ids=offenders,
            voucher_type="CONTRA",
        )


def _post_voucher_inline(conn, payload: VoucherInput) -> PostedVoucher:
    """Same as post_voucher() but reuses an already-open connection.

    Used by cancel_voucher() so the cancellation + reversal happen in
    the SAME transaction as the status update on the original. If we
    called post_voucher() directly it would open a second tx() and the
    atomicity guarantee would be lost.

    NOTE: kept private. The two code paths share the validation + FOR
    UPDATE + insert sequence but not the transaction boundary.
    """
    _validate_input_shape(payload)

    ledger_repo = LedgerRepository(conn)
    fy_repo = FinancialYearRepository(conn)
    series_repo = VoucherSeriesRepository(conn)
    voucher_repo = VoucherRepository(conn)

    fy = fy_repo.get_for_date(payload.voucher_date)

    if payload.voucher_series_id is None:
        series = series_repo.get_default_for(payload.voucher_type, fy["id"])
        if series is None:
            from ..errors import VoucherSeriesNotFoundError

            raise VoucherSeriesNotFoundError(
                f"No 'Default' voucher series for {payload.voucher_type} "
                f"in FY {fy['code']}.",
                voucher_type=payload.voucher_type,
                fy_code=fy["code"],
            )
        series_id = int(series["id"])
    else:
        series_id = payload.voucher_series_id

    ledger_ids = [ln.ledger_id for ln in payload.lines]
    if payload.party_ledger_id is not None:
        ledger_ids.append(payload.party_ledger_id)
    unique_ledger_ids = list({lid for lid in ledger_ids})
    ledger_repo.assert_all_belong_to_org(unique_ledger_ids)
    ledger_repo.assert_all_active(unique_ledger_ids)

    _validate_type_rules(conn, payload)
    total = _assert_balanced(payload.lines)

    voucher_number = series_repo.issue_next_number(series_id)

    try:
        voucher_id = voucher_repo.insert_header(
            voucher_type=payload.voucher_type,
            voucher_series_id=series_id,
            financial_year_id=int(fy["id"]),
            voucher_number=voucher_number,
            voucher_date=payload.voucher_date,
            reference_no=payload.reference_no,
            party_ledger_id=payload.party_ledger_id,
            narration=payload.narration,
            total_amount=total,
            source_doc_type=payload.source_doc_type,
            source_doc_id=payload.source_doc_id,
            created_by=payload.created_by,
        )
    except Exception as e:
        msg = str(e).lower()
        if "duplicate" in msg and "uq_voucher_number" in msg:
            raise VoucherDuplicateNumberError(
                f"Voucher number {voucher_number} already exists.",
                voucher_number=voucher_number,
            ) from e
        raise

    line_ids: list[int] = []
    for idx, ln in enumerate(payload.lines):
        line_id = voucher_repo.insert_line(
            voucher_id=voucher_id,
            ledger_id=ln.ledger_id,
            dr_cr=ln.dr_cr,
            amount=ln.amount,
            cost_center_id=ln.cost_center_id,
            line_narration=ln.line_narration,
            line_order=idx,
        )
        line_ids.append(line_id)

    return PostedVoucher(
        voucher_id=voucher_id,
        voucher_number=voucher_number,
        voucher_type=payload.voucher_type,
        total_amount=total,
        line_ids=line_ids,
    )
