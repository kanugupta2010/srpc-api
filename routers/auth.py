"""
routers/auth.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Auth endpoints:
  POST /auth/send-otp       — Send OTP to contractor mobile via MSG91
  POST /auth/verify-otp     — Verify OTP and issue JWT
  POST /auth/admin/login    — Admin login with username + password
  GET  /auth/me             — Current user profile (contractor or admin)
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from database import get_connection
from services.auth_service import (
    generate_otp, send_otp_msg91,
    create_otp_session, verify_otp_session,
    create_contractor_token, create_admin_token,
    verify_admin_credentials,
)
from services.dependencies import require_any, require_contractor

log = logging.getLogger(__name__)

router = APIRouter(tags=["Authentication"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SendOtpRequest(BaseModel):
    mobile: str


class VerifyOtpRequest(BaseModel):
    mobile: str
    otp:    str


class AdminLoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    role:         str


class MessageResponse(BaseModel):
    message: str


# ---------------------------------------------------------------------------
# POST /auth/send-otp
# ---------------------------------------------------------------------------

@router.post(
    "/send-otp",
    response_model=MessageResponse,
    summary="Send OTP to contractor mobile number",
)
def send_otp(req: SendOtpRequest, db=Depends(get_connection)):
    mobile = req.mobile.strip().replace(" ", "")

    # Validate mobile format
    if not mobile.isdigit() or len(mobile) != 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please enter a valid 10-digit mobile number.",
        )

    cursor = db.cursor(dictionary=True)

    # Check contractor exists
    cursor.execute(
        "SELECT id, status FROM contractors WHERE mobile = %s AND is_active = 1",
        (mobile,)
    )
    contractor = cursor.fetchone()

    if not contractor:
        cursor.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Mobile number not registered. Please contact the store.",
        )

    # Generate and send OTP
    otp = generate_otp()
    sent = send_otp_msg91(mobile, otp)

    if not sent:
        cursor.close()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not send OTP. Please try again in a moment.",
        )

    # Store hashed OTP
    create_otp_session(mobile, otp, cursor)
    db.commit()
    cursor.close()

    return {"message": f"OTP sent to {mobile[-4:].rjust(10, '*')}"}


# ---------------------------------------------------------------------------
# POST /auth/verify-otp
# ---------------------------------------------------------------------------

@router.post(
    "/verify-otp",
    response_model=TokenResponse,
    summary="Verify OTP and receive JWT token",
)
def verify_otp(req: VerifyOtpRequest, db=Depends(get_connection)):
    mobile = req.mobile.strip().replace(" ", "")
    otp    = req.otp.strip()

    cursor = db.cursor(dictionary=True)

    # Verify OTP
    success, message = verify_otp_session(mobile, otp, cursor)
    if not success:
        db.commit()   # save attempt counter
        cursor.close()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=message,
        )

    # Fetch contractor
    cursor.execute(
        "SELECT id, mobile, status FROM contractors WHERE mobile = %s AND is_active = 1",
        (mobile,)
    )
    contractor = cursor.fetchone()
    if not contractor:
        db.commit()
        cursor.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Contractor not found.",
        )

    # Update last login
    cursor.execute(
        "UPDATE contractors SET last_login_at = %s WHERE id = %s",
        (datetime.utcnow(), contractor["id"])
    )
    db.commit()
    cursor.close()

    token = create_contractor_token(contractor["id"], contractor["mobile"])
    return {
        "access_token": token,
        "token_type":   "bearer",
        "role":         "contractor",
    }


# ---------------------------------------------------------------------------
# POST /auth/admin/login
# ---------------------------------------------------------------------------

@router.post(
    "/admin/login",
    response_model=TokenResponse,
    summary="Admin login with username and password",
)
def admin_login(req: AdminLoginRequest):
    if not verify_admin_credentials(req.username, req.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
        )

    token = create_admin_token(req.username)
    return {
        "access_token": token,
        "token_type":   "bearer",
        "role":         "admin",
    }


# ---------------------------------------------------------------------------
# GET /auth/me
# ---------------------------------------------------------------------------

@router.get(
    "/me",
    summary="Get current user profile",
)
def get_me(payload: dict = Depends(require_any), db=Depends(get_connection)):
    role = payload.get("role")

    if role == "contractor":
        contractor_id = int(payload["sub"])
        cursor = db.cursor(dictionary=True)
        cursor.execute("""
            SELECT id, contractor_code, full_name, mobile,
                   business_name, status, tier, points_balance,
                   total_points_earned, approved_at, last_login_at
            FROM contractors
            WHERE id = %s AND is_active = 1
        """, (contractor_id,))
        contractor = cursor.fetchone()
        cursor.close()
        if not contractor:
            raise HTTPException(status_code=404, detail="Contractor not found.")
        return {"role": "contractor", "profile": contractor}

    elif role == "admin":
        return {"role": "admin", "profile": {"username": payload["sub"]}}

    raise HTTPException(status_code=403, detail="Unknown role.")