"""
services/dependencies.py
SRPC Enterprises Private Limited — Saraswati Loyalty Program

FastAPI dependencies for JWT authentication.
Used by all protected routes via Depends().

Usage:
    @router.get("/me")
    def get_me(payload = Depends(require_contractor)):
        contractor_id = payload["sub"]

    @router.get("/admin/something")
    def admin_route(payload = Depends(require_admin)):
        ...
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from services.auth_service import decode_jwt

bearer_scheme = HTTPBearer()


def _get_token_payload(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    """Extract and validate JWT from Authorization: Bearer <token> header."""
    payload = decode_jwt(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


def require_contractor(payload: dict = Depends(_get_token_payload)) -> dict:
    """Dependency — only allows contractor-role JWTs."""
    if payload.get("role") != "contractor":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Contractor access required.",
        )
    return payload


def require_admin(payload: dict = Depends(_get_token_payload)) -> dict:
    """Dependency — only allows admin-role JWTs."""
    if payload.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )
    return payload


def require_any(payload: dict = Depends(_get_token_payload)) -> dict:
    """Dependency — allows any valid JWT (contractor or admin)."""
    return payload