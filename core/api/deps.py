"""FastAPI dependency to bind active_org_id for every core-API request.

Single-tenant shortcut for the current deployment: after `require_admin`
passes, we bind organization_id = 1 (SRPC) for the request lifetime.

When multi-tenancy goes live:
    * Extend the admin JWT to carry `active_org_id` (and optionally
      memberships for an org switcher).
    * Replace the contextvar binding below with
      `int(payload['active_org_id'])`.
    * Nothing else in core/ changes.

The existing admin JWT (from services/auth_service.create_admin_token)
doesn't know about orgs today. Rather than touch the shared auth flow
(which the contractor app and admin dashboard both depend on), we
isolate the change to this one file.
"""
from __future__ import annotations

from fastapi import Depends, HTTPException, status

from services.dependencies import require_admin  # existing loyalty-program dep
from ..tenancy import _active_org_id

# Phase C / single-tenant. Revisit when the admin JWT learns about orgs.
SINGLE_TENANT_ORG_ID = 1


def bind_active_org(
    payload: dict = Depends(require_admin),
) -> int:
    """Require admin auth, bind SRPC org to the request context.

    Yields the active_org_id so routers can log it if they want. The
    contextvar binding survives across await boundaries inside the
    same request.
    """
    # Phase C: single-tenant. Read from JWT later.
    org_id = SINGLE_TENANT_ORG_ID

    # Bind for the duration of this request. FastAPI's contextvar
    # support handles async task boundaries; we don't need to reset
    # because each HTTP request runs in its own task.
    _active_org_id.set(org_id)

    # Surface admin identity for audit_log writes.
    # Attach to the returned value via a tiny dict — keeps typing simple.
    return org_id


def current_admin(
    payload: dict = Depends(require_admin),
) -> str:
    """Return the admin username from the JWT, for audit trails."""
    return payload.get("sub") or "admin"
