"""
models/schemas.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program
Pydantic request/response models
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Import pipeline
# ---------------------------------------------------------------------------

class ImportSummaryResponse(BaseModel):
    """Response returned after a CSV import completes."""
    batch_id:            int
    filename:            str
    status:              str
    total_rows:          int
    invoices_found:      int
    invoices_imported:   int
    invoices_skipped:    int
    points_awarded:      Decimal
    date_from:           Optional[date]
    date_to:             Optional[date]
    notes:               Optional[str]
    created_at:          datetime

    class Config:
        from_attributes = True


class ImportListResponse(BaseModel):
    """One row in the import history list."""
    id:                  int
    filename:            str
    imported_by:         str
    date_from:           Optional[date]
    date_to:             Optional[date]
    invoices_imported:   int
    invoices_skipped:    int
    points_awarded:      Decimal
    status:              str
    notes:               Optional[str]
    created_at:          datetime

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Contractor
# ---------------------------------------------------------------------------

class ContractorSummaryResponse(BaseModel):
    id:                        int
    contractor_code:           str
    full_name:                 str
    mobile:                    str
    business_name:             Optional[str]
    status:                    str
    tier:                      str
    points_balance:            Decimal
    total_points_earned:       Decimal
    total_points_redeemed:     Decimal
    total_points_expired:      Decimal
    points_expiring_in_30_days: Decimal
    next_tier_at:              Optional[Decimal]
    approved_at:               Optional[datetime]
    last_login_at:             Optional[datetime]

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Generic responses
# ---------------------------------------------------------------------------

class MessageResponse(BaseModel):
    message: str


class ErrorResponse(BaseModel):
    detail: str