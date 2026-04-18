"""Pydantic v2 schemas for settlement endpoints."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


Money = Annotated[Decimal, Field(gt=Decimal("0"), max_digits=18, decimal_places=2)]


class _ApiBase(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="forbid",
    )


class AllocationRequest(_ApiBase):
    """One allocation line inside a settlement request."""

    bill_reference_id: int = Field(gt=0)
    amount: Money


class AllocateRequest(_ApiBase):
    """Body for POST /api/v1/settlement/allocate."""

    allocating_voucher_id: int = Field(gt=0)
    allocations: list[AllocationRequest] = Field(min_length=1)


class AllocationResponse(_ApiBase):
    id: int
    allocating_voucher_id: int
    bill_reference_id: int
    bill_no: str
    amount: Decimal
    allocated_at: str


class BillReferenceResponse(_ApiBase):
    id: int
    party_ledger_id: int
    party_ledger_name: Optional[str] = None
    bill_no: str
    bill_date: date
    due_date: Optional[date] = None
    original_amount: Decimal
    outstanding_amount: Decimal
    side: Literal["RECEIVABLE", "PAYABLE"]
    status: Literal["OPEN", "PARTIAL", "CLEARED", "WRITTEN_OFF"]
    source_voucher_id: Optional[int] = None


# ----- Bootstrap an opening-balance bill (used until Phase E ships) ------

class OpeningBillRequest(_ApiBase):
    """Create a bill_reference without a source invoice voucher.

    Used to seed opening outstanding bills during migration from Busy.
    Phase E's sales/purchase invoice endpoints will stop needing this —
    they'll create bill_references as a side-effect.
    """

    party_ledger_id: int = Field(gt=0)
    bill_no: str = Field(min_length=1, max_length=40)
    bill_date: date
    due_date: Optional[date] = None
    amount: Money
    side: Literal["RECEIVABLE", "PAYABLE"]
    notes: Optional[str] = Field(default=None, max_length=500)
