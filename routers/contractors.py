"""
routers/contractors.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Contractor endpoints (MVP scope):
  GET /contractors/me/summary        — Points balance, tier, profile
  GET /contractors/me/points-history — Paginated points ledger
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from decimal import Decimal
from datetime import datetime, date

from database import get_connection
from services.dependencies import require_contractor

log = logging.getLogger(__name__)

router = APIRouter(tags=["Contractors"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class ContractorSummary(BaseModel):
    # Profile
    id:                         int
    contractor_code:            str
    full_name:                  str
    mobile:                     str
    business_name:              Optional[str]
    status:                     str
    approved_at:                Optional[datetime]

    # Tier
    tier:                       str
    next_tier_at:               Optional[Decimal]
    points_to_next_tier:        Optional[Decimal]

    # Points
    points_balance:             Decimal
    total_points_earned:        Decimal
    total_points_redeemed:      Decimal
    total_points_expired:       Decimal
    points_expiring_in_30_days: Decimal

    class Config:
        from_attributes = True


class PointsHistoryEntry(BaseModel):
    id:             int
    event_type:     str
    points:         Decimal
    bill_number:    Optional[str]
    invoice_date:   Optional[date]
    invoice_type:   Optional[str]
    expires_at:     Optional[datetime]
    is_expired:     int
    notes:          Optional[str]
    created_at:     datetime

    class Config:
        from_attributes = True


class PointsHistoryResponse(BaseModel):
    page:        int
    page_size:   int
    total:       int
    entries:     list[PointsHistoryEntry]


# ---------------------------------------------------------------------------
# GET /contractors/me/summary
# ---------------------------------------------------------------------------

@router.get(
    "/me/summary",
    response_model=ContractorSummary,
    summary="Get contractor points summary and profile",
)
def get_summary(
    payload: dict = Depends(require_contractor),
    db=Depends(get_connection),
):
    contractor_id = int(payload["sub"])
    cursor = db.cursor(dictionary=True)

    # Fetch from vw_contractor_summary
    cursor.execute(
        "SELECT * FROM vw_contractor_summary WHERE id = %s",
        (contractor_id,)
    )
    row = cursor.fetchone()

    if not row:
        cursor.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Contractor not found.",
        )

    # Calculate points to next tier
    points_to_next = None
    if row["next_tier_at"] is not None:
        points_to_next = max(Decimal("0"), Decimal(str(row["next_tier_at"])) - row["total_points_earned"])

    cursor.close()

    return {
        "id":                         row["id"],
        "contractor_code":            row["contractor_code"],
        "full_name":                  row["full_name"],
        "mobile":                     row["mobile"],
        "business_name":              row["business_name"],
        "status":                     row["status"],
        "approved_at":                row["approved_at"],
        "tier":                       row["tier"],
        "next_tier_at":               row["next_tier_at"],
        "points_to_next_tier":        points_to_next,
        "points_balance":             row["points_balance"],
        "total_points_earned":        row["total_points_earned"],
        "total_points_redeemed":      row["total_points_redeemed"],
        "total_points_expired":       row["total_points_expired"],
        "points_expiring_in_30_days": row["points_expiring_in_30_days"],
    }


# ---------------------------------------------------------------------------
# GET /contractors/me/points-history
# ---------------------------------------------------------------------------

@router.get(
    "/me/points-history",
    response_model=PointsHistoryResponse,
    summary="Get paginated points history",
)
def get_points_history(
    page:      int = Query(default=1, ge=1, description="Page number"),
    page_size: int = Query(default=20, ge=1, le=100, description="Records per page"),
    payload:   dict = Depends(require_contractor),
    db=Depends(get_connection),
):
    contractor_id = int(payload["sub"])
    offset = (page - 1) * page_size

    cursor = db.cursor(dictionary=True)

    # Total count
    cursor.execute(
        "SELECT COUNT(*) AS total FROM vw_points_ledger WHERE contractor_id = %s",
        (contractor_id,)
    )
    total = cursor.fetchone()["total"]

    # Paginated entries
    cursor.execute("""
        SELECT
            id,
            event_type,
            points,
            bill_number,
            invoice_date,
            invoice_type,
            expires_at,
            is_expired,
            notes,
            created_at
        FROM vw_points_ledger
        WHERE contractor_id = %s
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, (contractor_id, page_size, offset))

    entries = cursor.fetchall()
    cursor.close()

    return {
        "page":      page,
        "page_size": page_size,
        "total":     total,
        "entries":   entries,
    }