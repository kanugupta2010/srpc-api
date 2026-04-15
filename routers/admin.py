"""
routers/admin.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Admin endpoints:
  POST /admin/import          — Upload Busy 21 CSV, parse, award points
  GET  /admin/imports         — List past import batches
  GET  /admin/imports/{id}    — Single import batch detail
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status

from database import get_connection as get_db_connection
from models.schemas import ImportListResponse, ImportSummaryResponse
from services.import_service import parse_file, resolve_contractors
from services.points_engine import process_invoices
from services.dependencies import require_admin

log = logging.getLogger(__name__)

router = APIRouter(tags=["Admin"])

# ---------------------------------------------------------------------------
# POST /admin/import
# ---------------------------------------------------------------------------

@router.post(
    "/import",
    response_model=ImportSummaryResponse,
    summary="Upload Busy 21 CSV and process points",
    status_code=status.HTTP_201_CREATED,
)
async def import_csv(
    file:    UploadFile = File(...),
    payload: dict = Depends(require_admin),
    db=Depends(get_db_connection),
):
    """
    Upload a CSV exported from Busy 21.
    - Parses invoices and line items
    - Resolves contractor attribution
    - Calculates and awards points
    - Returns import summary
    """
    # --- Validate file type ---
    if not file.filename.lower().endswith((".csv", ".xlsx")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are accepted.",
        )

    content = await file.read()
    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file is empty.",
        )

    # --- Create import_batches record (status = processing) ---
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        INSERT INTO import_batches (filename, imported_by, status)
        VALUES (%s, 'admin', 'processing')
    """, (file.filename,))
    db.commit()
    batch_id = cursor.lastrowid
    cursor.close()

    try:
        # --- Parse CSV ---
        try:
            invoices, parse_stats = parse_file(content, file.filename)
        except ValueError as exc:
            _fail_batch(db, batch_id, str(exc))
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            )

        if not invoices:
            _complete_batch(db, batch_id, {
                "total_rows": parse_stats["total_rows"],
                "invoices_found": 0,
                "invoices_imported": 0,
                "invoices_skipped": 0,
                "points_awarded": 0.0,
                "date_from": None,
                "date_to": None,
                "notes": "No invoices found in file.",
            })
            return _fetch_batch(db, batch_id)

        # --- Resolve contractors ---
        resolve_contractors(invoices, db)

        # --- Process invoices + award points ---
        counters = process_invoices(invoices, batch_id, db)

        # --- Determine date range from parsed invoices ---
        dates = [inv.invoice_date for inv in invoices if inv.invoice_date]
        date_from = min(dates) if dates else None
        date_to   = max(dates) if dates else None

        # --- Update batch record to completed ---
        final_status = (
            "completed_with_errors" if counters.get("errors", 0) > 0
            else "completed"
        )
        _complete_batch(db, batch_id, {
            "total_rows":         parse_stats["total_rows"],
            "invoices_found":     len(invoices),
            "invoices_imported":  counters["invoices_imported"],
            "invoices_skipped":   counters["invoices_skipped"],
            "points_awarded":     round(counters["points_awarded"], 2),
            "date_from":          date_from,
            "date_to":            date_to,
            "notes":              counters.get("notes"),
            "status":             final_status,
        })

        log.info(
            "Import batch %d complete — imported: %d, skipped: %d, points: %.2f",
            batch_id,
            counters["invoices_imported"],
            counters["invoices_skipped"],
            counters["points_awarded"],
        )

        return _fetch_batch(db, batch_id)

    except HTTPException:
        raise
    except Exception as exc:
        log.exception("Import batch %d failed: %s", batch_id, exc)
        _fail_batch(db, batch_id, str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Import failed: {exc}",
        )


# ---------------------------------------------------------------------------
# GET /admin/imports
# ---------------------------------------------------------------------------

@router.get(
    "/imports",
    summary="List all import batches",
)
def list_imports(
    page:      int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    payload:   dict = Depends(require_admin),
    db=Depends(get_db_connection),
):
    """Returns import history, most recent first."""
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(*) AS total FROM import_batches")
    total = cursor.fetchone()["total"]
    offset = (page - 1) * page_size
    cursor.execute("""
        SELECT id, filename, imported_by, status,
               total_rows, invoices_found, invoices_imported,
               invoices_skipped, points_awarded,
               date_from, date_to, notes, created_at
        FROM import_batches
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, (page_size, offset))
    batches = cursor.fetchall()
    cursor.close()
    return {"page": page, "page_size": page_size, "total": total, "batches": batches}


# ---------------------------------------------------------------------------
# GET /admin/imports/{batch_id}
# ---------------------------------------------------------------------------

@router.get(
    "/imports/{batch_id}",
    summary="Get single import batch detail",
)
def get_import(
    batch_id: int,
    payload:  dict = Depends(require_admin),
    db=Depends(get_db_connection),
):
    batch = _fetch_batch(db, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Import batch not found.")
    return batch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_batch(db, batch_id: int):
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM import_batches WHERE id = %s", (batch_id,)
    )
    row = cursor.fetchone()
    cursor.close()
    return row


def _complete_batch(db, batch_id: int, data: dict) -> None:
    cursor = db.cursor()
    cursor.execute("""
        UPDATE import_batches SET
            total_rows        = %s,
            invoices_found    = %s,
            invoices_imported = %s,
            invoices_skipped  = %s,
            points_awarded    = %s,
            date_from         = %s,
            date_to           = %s,
            notes             = %s,
            status            = %s
        WHERE id = %s
    """, (
        data.get("total_rows", 0),
        data.get("invoices_found", 0),
        data.get("invoices_imported", 0),
        data.get("invoices_skipped", 0),
        data.get("points_awarded", 0.0),
        data.get("date_from"),
        data.get("date_to"),
        data.get("notes"),
        data.get("status", "completed"),
        batch_id,
    ))
    db.commit()
    cursor.close()


def _fail_batch(db, batch_id: int, error_msg: str) -> None:
    cursor = db.cursor()
    cursor.execute("""
        UPDATE import_batches SET status = 'failed', notes = %s WHERE id = %s
    """, (error_msg[:500], batch_id))
    db.commit()
    cursor.close()


# ─── Contractors list ────────────────────────────────────────────────────────


@router.get(
    "/contractors",
    summary="List all contractors (admin)",
)
def list_contractors(
    page:      int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    status:    Optional[str] = Query(default=None),
    payload:   dict = Depends(require_admin),
    db=Depends(get_db_connection),
):
    cursor = db.cursor(dictionary=True)

    where = ["company_code = 'SRPC'"]
    params = []
    if status:
        where.append("status = %s")
        params.append(status)

    where_clause = " AND ".join(where)
    cursor.execute(f"SELECT COUNT(*) AS total FROM contractors WHERE {where_clause}", params)
    total = cursor.fetchone()["total"]

    offset = (page - 1) * page_size
    cursor.execute(f"""
        SELECT id, contractor_code, full_name, business_name, mobile,
               status, tier, points_balance, total_points_earned,
               total_points_redeemed, approved_at, last_login_at, created_at
        FROM contractors
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, params + [page_size, offset])
    contractors = cursor.fetchall()
    cursor.close()

    return {"page": page, "page_size": page_size, "total": total, "contractors": contractors}