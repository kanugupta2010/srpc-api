"""
services/auth_service.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

Handles:
  - OTP generation and hashing
  - MSG91 OTP sending
  - JWT token creation and validation
  - Admin credential verification
"""

import os
import random
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
from jose import JWTError, jwt
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config from .env
# ---------------------------------------------------------------------------
JWT_SECRET          = os.getenv("JWT_SECRET", "change-this-secret")
JWT_ALGORITHM       = "HS256"
JWT_EXPIRE_DAYS     = int(os.getenv("JWT_EXPIRE_DAYS", 30))

MSG91_API_KEY       = os.getenv("MSG91_API_KEY", "")
MSG91_TEMPLATE_ID   = os.getenv("MSG91_TEMPLATE_ID", "")
MSG91_SENDER_ID     = os.getenv("MSG91_SENDER_ID", "SRPCRL")

ADMIN_USERNAME      = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH", "")  # sha256 hex of password

OTP_EXPIRY_MINUTES  = 10
OTP_MAX_ATTEMPTS    = 3


# ---------------------------------------------------------------------------
# OTP helpers
# ---------------------------------------------------------------------------

def generate_otp() -> str:
    """Generate a 6-digit OTP."""
    return str(random.randint(100000, 999999))


def hash_otp(otp: str) -> str:
    """SHA-256 hash of OTP — never store plain."""
    return hashlib.sha256(otp.encode()).hexdigest()


def hash_password(password: str) -> str:
    """SHA-256 hash for admin password comparison."""
    return hashlib.sha256(password.encode()).hexdigest()


# ---------------------------------------------------------------------------
# MSG91 OTP sender
# ---------------------------------------------------------------------------

def send_otp_msg91(mobile: str, otp: str) -> bool:
    """
    Send OTP via MSG91 Send OTP API.
    Returns True on success, False on failure.
    Mobile should be 10-digit Indian number — API prepends 91.
    """
    if not MSG91_API_KEY or not MSG91_TEMPLATE_ID:
        log.error("MSG91_API_KEY or MSG91_TEMPLATE_ID not configured in .env")
        return False

    url = "https://control.msg91.com/api/v5/otp"
    payload = {
        "template_id": MSG91_TEMPLATE_ID,
        "mobile":      f"91{mobile}",   # MSG91 requires country code prefix
        "authkey":     MSG91_API_KEY,
        "otp":         otp,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        data = resp.json()
        if data.get("type") == "success":
            log.info("OTP sent successfully to %s", mobile)
            return True
        else:
            log.error("MSG91 error for %s: %s", mobile, data)
            return False
    except requests.RequestException as exc:
        log.error("MSG91 request failed for %s: %s", mobile, exc)
        return False


# ---------------------------------------------------------------------------
# OTP session management
# ---------------------------------------------------------------------------

def create_otp_session(mobile: str, otp: str, cursor) -> None:
    """
    Delete any existing OTP session for this mobile and create a fresh one.
    OTP is stored hashed — never plain.
    """
    expires_at = datetime.utcnow() + timedelta(minutes=OTP_EXPIRY_MINUTES)
    otp_hash   = hash_otp(otp)

    # Remove any existing session for this mobile
    cursor.execute("DELETE FROM otp_sessions WHERE mobile = %s", (mobile,))

    cursor.execute("""
        INSERT INTO otp_sessions (mobile, otp_hash, expires_at, attempts, is_used)
        VALUES (%s, %s, %s, 0, 0)
    """, (mobile, otp_hash, expires_at))


def verify_otp_session(mobile: str, otp: str, cursor) -> tuple[bool, str]:
    """
    Verify OTP for a given mobile.
    Returns (success: bool, message: str).
    Increments attempt counter. Marks session as used on success.
    """
    cursor.execute("""
        SELECT id, otp_hash, expires_at, attempts, is_used
        FROM otp_sessions
        WHERE mobile = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (mobile,))
    session = cursor.fetchone()

    if not session:
        return False, "No OTP found for this number. Please request a new OTP."

    if session["is_used"]:
        return False, "OTP already used. Please request a new OTP."

    if datetime.utcnow() > session["expires_at"]:
        cursor.execute("DELETE FROM otp_sessions WHERE id = %s", (session["id"],))
        return False, "OTP has expired. Please request a new OTP."

    if session["attempts"] >= OTP_MAX_ATTEMPTS:
        cursor.execute("DELETE FROM otp_sessions WHERE id = %s", (session["id"],))
        return False, "Maximum attempts exceeded. Please request a new OTP."

    # Increment attempt counter
    cursor.execute(
        "UPDATE otp_sessions SET attempts = attempts + 1 WHERE id = %s",
        (session["id"],)
    )

    if hash_otp(otp) != session["otp_hash"]:
        remaining = OTP_MAX_ATTEMPTS - (session["attempts"] + 1)
        return False, f"Incorrect OTP. {remaining} attempt(s) remaining."

    # Mark as used
    cursor.execute(
        "UPDATE otp_sessions SET is_used = 1 WHERE id = %s",
        (session["id"],)
    )
    return True, "OTP verified."


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------

def create_jwt(payload: dict) -> str:
    """Create a signed JWT with expiry."""
    data = payload.copy()
    data["exp"] = datetime.utcnow() + timedelta(days=JWT_EXPIRE_DAYS)
    return jwt.encode(data, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_jwt(token: str) -> Optional[dict]:
    """Decode and validate a JWT. Returns payload dict or None."""
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None


def create_contractor_token(contractor_id: int, mobile: str) -> str:
    return create_jwt({
        "sub":           str(contractor_id),
        "mobile":        mobile,
        "role":          "contractor",
    })


def create_admin_token(username: str) -> str:
    return create_jwt({
        "sub":      username,
        "role":     "admin",
    })


# ---------------------------------------------------------------------------
# Admin auth
# ---------------------------------------------------------------------------

def verify_admin_credentials(username: str, password: str) -> bool:
    """Verify admin username and password against .env values."""
    if username != ADMIN_USERNAME:
        return False
    if not ADMIN_PASSWORD_HASH:
        log.error("ADMIN_PASSWORD_HASH not set in .env")
        return False
    return hash_password(password) == ADMIN_PASSWORD_HASH