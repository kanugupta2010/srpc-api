"""Pydantic v2 schemas for party + ledger endpoints."""
from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _ApiBase(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="forbid",
    )


class CreatePartyRequest(_ApiBase):
    """Creates a party AND its linked ledger in one transaction."""

    party_type: Literal["CUSTOMER", "SUPPLIER", "BOTH"] = "CUSTOMER"
    name: str = Field(min_length=1, max_length=200)
    display_name: Optional[str] = Field(default=None, max_length=200)
    mobile: Optional[str] = Field(default=None, max_length=20)
    email: Optional[str] = Field(default=None, max_length=120)
    gstin: Optional[str] = Field(default=None, max_length=15)
    pan: Optional[str] = Field(default=None, max_length=10)
    state_code: Optional[str] = Field(default=None, max_length=2)
    address_line1: Optional[str] = Field(default=None, max_length=200)
    address_line2: Optional[str] = Field(default=None, max_length=200)
    city: Optional[str] = Field(default=None, max_length=80)
    pincode: Optional[str] = Field(default=None, max_length=10)
    credit_limit: Optional[Annotated[Decimal, Field(ge=Decimal("0"))]] = None
    credit_days: Optional[int] = Field(default=None, ge=0, le=365)
    opening_balance: Annotated[
        Decimal, Field(max_digits=18, decimal_places=2)
    ] = Decimal("0.00")
    # opening_balance_dr_cr indicates the sign of opening_balance.
    # Ignored when opening_balance == 0.
    opening_balance_dr_cr: Literal["Dr", "Cr"] = "Dr"

    @field_validator("gstin")
    @classmethod
    def _gstin_length(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        if len(v) != 15:
            raise ValueError("gstin must be exactly 15 characters")
        return v.upper()


class PartyResponse(_ApiBase):
    id: int
    party_type: str
    name: str
    display_name: Optional[str] = None
    mobile: Optional[str] = None
    email: Optional[str] = None
    gstin: Optional[str] = None
    state_code: Optional[str] = None
    ledger_id: Optional[int] = None
    ledger_name: Optional[str] = None
    is_active: bool


class LedgerResponse(_ApiBase):
    id: int
    group_id: int
    group_name: Optional[str] = None
    name: str
    opening_balance: Decimal
    is_party: bool
    is_bank: bool
    is_reserved: bool
    is_active: bool
