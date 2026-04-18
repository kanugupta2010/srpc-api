"""Pydantic v2 schemas for the voucher endpoints.

Each voucher type gets a typed request schema that encodes its structural
rules (e.g. PaymentVoucherRequest requires a cash/bank ledger on the Cr
side). The service layer's posting_service accepts only the generic
VoucherInput — these Pydantic shapes translate user intent into that
DTO, so invalid combinations are rejected at the API boundary before
they reach the posting engine.

All monetary fields are Decimal; Pydantic v2 rejects floats automatically
via the field-validator.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ----- Shared field annotations -------------------------------------------

Money = Annotated[Decimal, Field(gt=Decimal("0"), max_digits=18, decimal_places=2)]
LedgerId = Annotated[int, Field(gt=0)]
Narration = Annotated[str, Field(max_length=2000)]


class _ApiBase(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="forbid",
        use_enum_values=True,
    )


# ----- Line inputs --------------------------------------------------------

class VoucherLineRequest(_ApiBase):
    """Generic Dr/Cr line used by Journal vouchers (and as substrate for
    the specialised voucher types below)."""

    ledger_id: LedgerId
    dr_cr: Literal["Dr", "Cr"]
    amount: Money
    cost_center_id: Optional[int] = None
    line_narration: Optional[Narration] = None


# ----- Payment voucher ----------------------------------------------------

class PaymentLineRequest(_ApiBase):
    """One Dr line (expense / creditor being settled)."""

    ledger_id: LedgerId
    amount: Money
    cost_center_id: Optional[int] = None
    line_narration: Optional[Narration] = None


class PaymentVoucherRequest(_ApiBase):
    """Money going out: Dr expense/creditor, Cr cash/bank.

    One or more Dr lines may be supplied (e.g. pay rent + electricity
    from the same bank transfer). Exactly one Cr ledger (cash or bank)
    is specified; its amount equals the sum of all Dr amounts.
    """

    voucher_date: date
    paid_from_ledger_id: LedgerId  # Cash or Bank
    lines: list[PaymentLineRequest] = Field(min_length=1)
    party_ledger_id: Optional[LedgerId] = None
    reference_no: Optional[str] = Field(default=None, max_length=60)
    narration: Optional[Narration] = None

    @field_validator("lines")
    @classmethod
    def _no_duplicate_ledgers(cls, v: list[PaymentLineRequest]) -> list:
        ids = [ln.ledger_id for ln in v]
        if len(ids) != len(set(ids)):
            raise ValueError(
                "Duplicate ledger_id in payment lines; combine the amounts."
            )
        return v


# ----- Receipt voucher ----------------------------------------------------

class ReceiptLineRequest(_ApiBase):
    """One Cr line (debtor / income)."""

    ledger_id: LedgerId
    amount: Money
    cost_center_id: Optional[int] = None
    line_narration: Optional[Narration] = None


class ReceiptVoucherRequest(_ApiBase):
    """Money coming in: Dr cash/bank, Cr debtor/income."""

    voucher_date: date
    received_into_ledger_id: LedgerId  # Cash or Bank
    lines: list[ReceiptLineRequest] = Field(min_length=1)
    party_ledger_id: Optional[LedgerId] = None
    reference_no: Optional[str] = Field(default=None, max_length=60)
    narration: Optional[Narration] = None

    # Optional auto-allocation — see settlement_router for standalone
    # allocation too. When present, the receipt is allocated to the
    # listed bills in the same transaction.
    allocate_to_bills: Optional[list["AllocationRequest"]] = None

    @field_validator("lines")
    @classmethod
    def _no_duplicate_ledgers(cls, v: list[ReceiptLineRequest]) -> list:
        ids = [ln.ledger_id for ln in v]
        if len(ids) != len(set(ids)):
            raise ValueError(
                "Duplicate ledger_id in receipt lines; combine the amounts."
            )
        return v


# ----- Journal voucher ----------------------------------------------------

class JournalVoucherRequest(_ApiBase):
    """Fully-general multi-line Dr/Cr voucher.

    The service layer enforces Σ Dr == Σ Cr. Use this for adjustments,
    accruals, depreciation, opening balances, etc.
    """

    voucher_date: date
    lines: list[VoucherLineRequest] = Field(min_length=2)
    party_ledger_id: Optional[LedgerId] = None
    reference_no: Optional[str] = Field(default=None, max_length=60)
    narration: Optional[Narration] = None


# ----- Contra voucher -----------------------------------------------------

class ContraVoucherRequest(_ApiBase):
    """Cash <-> Bank or Bank <-> Bank movement.

    The posting service enforces that all referenced ledgers belong to
    the Cash-in-Hand or Bank Accounts reserved groups.
    """

    voucher_date: date
    from_ledger_id: LedgerId
    to_ledger_id: LedgerId
    amount: Money
    reference_no: Optional[str] = Field(default=None, max_length=60)
    narration: Optional[Narration] = None

    @field_validator("to_ledger_id")
    @classmethod
    def _distinct_ledgers(cls, v: int, info) -> int:
        if info.data.get("from_ledger_id") == v:
            raise ValueError("from_ledger_id and to_ledger_id must differ")
        return v


# ----- Cancel voucher -----------------------------------------------------

class CancelVoucherRequest(_ApiBase):
    reason: Optional[str] = Field(default=None, max_length=500)


# ----- Response shapes ----------------------------------------------------

class VoucherLineResponse(_ApiBase):
    id: int
    ledger_id: int
    ledger_name: Optional[str] = None
    dr_cr: Literal["Dr", "Cr"]
    amount: Decimal
    cost_center_id: Optional[int] = None
    line_narration: Optional[str] = None
    line_order: int


class VoucherResponse(_ApiBase):
    id: int
    voucher_type: str
    voucher_number: str
    voucher_date: date
    reference_no: Optional[str] = None
    party_ledger_id: Optional[int] = None
    party_ledger_name: Optional[str] = None
    narration: Optional[str] = None
    total_amount: Decimal
    status: Literal["DRAFT", "POSTED", "CANCELLED"]
    source_doc_type: Optional[str] = None
    source_doc_id: Optional[int] = None
    lines: list[VoucherLineResponse] = Field(default_factory=list)


class PostedVoucherResponse(_ApiBase):
    """Summary returned immediately after a successful post."""

    voucher_id: int
    voucher_number: str
    voucher_type: str
    total_amount: Decimal


# Forward-ref resolution for ReceiptVoucherRequest.allocate_to_bills.
from .settlement_schemas import AllocationRequest  # noqa: E402

ReceiptVoucherRequest.model_rebuild()
