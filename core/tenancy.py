"""Tenancy context for the accounting core.

The active organization_id is stored in a contextvars.ContextVar so it
flows through async/await boundaries cleanly. The FastAPI dependency
`bind_active_org` extracts it from the JWT (Phase 6 already issues JWTs
for the loyalty admin login; future work: add active_org_id claim).

For the immediate Phase A+B scaffolding, organization resolution falls
back to the SRPC default (org_id=1) when no JWT is present, so unit
tests and the seed script can run without auth setup. Production code
paths must always go through the JWT-derived dependency.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional

from .errors import TenancyContextMissingError

# Module-level contextvar. Defaults to None so missing context is loud.
_active_org_id: ContextVar[Optional[int]] = ContextVar(
    "srpc_active_org_id", default=None
)

# Default org for development / seed scripts. Production code MUST set
# the contextvar via the FastAPI dependency, never rely on this.
DEFAULT_DEV_ORG_ID = 1


def get_active_org_id() -> int:
    """Return the active organization_id or raise.

    Use this in repositories and services. Never accept org_id from
    request bodies.
    """
    val = _active_org_id.get()
    if val is None:
        raise TenancyContextMissingError(
            "No organization_id bound to the current request/task. "
            "Use bind_active_org() or the FastAPI dependency."
        )
    return val


@contextmanager
def bind_org(org_id: int) -> Iterator[None]:
    """Bind organization_id for the duration of a block.

    Usage::

        with bind_org(1):
            posting_service.post_voucher(...)
    """
    if org_id is None or org_id <= 0:
        raise ValueError(f"Invalid organization_id: {org_id!r}")
    token = _active_org_id.set(org_id)
    try:
        yield
    finally:
        _active_org_id.reset(token)


def _set_org_id_for_dev(org_id: int = DEFAULT_DEV_ORG_ID) -> None:
    """Test/dev helper. Production code path must not call this."""
    _active_org_id.set(org_id)


# --- FastAPI dependency ---------------------------------------------------
# Wired up in routers when Phase C ships. Kept here so the contract is
# defined alongside the contextvar.
def fastapi_active_org_dep():
    """Returns a FastAPI dependency that binds active_org_id from the JWT.

    Resolved lazily so the core package stays importable in test contexts
    without FastAPI installed (it is, but this keeps the boundary clean).
    """
    from fastapi import Depends, HTTPException, status

    # Reuse the existing JWT decoder from services/dependencies.py once
    # Phase C wires this up. For now this is a placeholder that future
    # routers will use:
    #
    #     active_org_id: int = Depends(fastapi_active_org_dep())
    #
    def _dep(token_payload: dict = Depends(_decode_jwt_placeholder)) -> int:
        org_id = token_payload.get("active_org_id")
        if not org_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"code": "NO_ACTIVE_ORG"},
            )
        _active_org_id.set(int(org_id))
        return int(org_id)

    return _dep


def _decode_jwt_placeholder() -> dict:  # pragma: no cover
    """Replaced when Phase C wires routers to the existing JWT decoder."""
    raise NotImplementedError(
        "fastapi_active_org_dep requires Phase C wiring to the JWT decoder."
    )
