"""FastAPI exception handlers for the accounting core.

Translates core.errors.DomainError subclasses into the uniform
{ code, message, details } response envelope from CloudAccountingDesign
§17.3. HTTP status is chosen per domain error class.

Register in main.py:

    from core.api.errors import register_error_handlers
    register_error_handlers(app)
"""
from __future__ import annotations

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from ..errors import (
    CrossOrgReferenceError,
    DomainError,
    FinancialYearNotFoundError,
    LedgerInactiveError,
    TenancyContextMissingError,
    VoucherAlreadyCancelledError,
    VoucherDuplicateNumberError,
    VoucherImmutableError,
    VoucherNotPostedError,
    VoucherPeriodLockedError,
    VoucherSeriesNotFoundError,
    VoucherTypeRuleViolation,
    VoucherUnbalancedError,
)

# Map concrete DomainError subclasses to HTTP status codes.
# Anything not listed defaults to 400 (Bad Request).
_STATUS_MAP: dict[type, int] = {
    # 400 — client supplied bad data the domain rejects
    VoucherUnbalancedError:       status.HTTP_400_BAD_REQUEST,
    VoucherTypeRuleViolation:     status.HTTP_400_BAD_REQUEST,
    LedgerInactiveError:          status.HTTP_400_BAD_REQUEST,

    # 404 — referenced entity does not exist in active org
    FinancialYearNotFoundError:   status.HTTP_404_NOT_FOUND,
    VoucherSeriesNotFoundError:   status.HTTP_404_NOT_FOUND,
    VoucherNotPostedError:        status.HTTP_404_NOT_FOUND,

    # 409 — conflict with current state
    VoucherDuplicateNumberError:  status.HTTP_409_CONFLICT,
    VoucherAlreadyCancelledError: status.HTTP_409_CONFLICT,
    VoucherImmutableError:        status.HTTP_409_CONFLICT,
    VoucherPeriodLockedError:     status.HTTP_409_CONFLICT,

    # 403 — caller somehow referenced another org's data
    CrossOrgReferenceError:       status.HTTP_403_FORBIDDEN,

    # 500 — our bug: core ran without an org bound
    TenancyContextMissingError:   status.HTTP_500_INTERNAL_SERVER_ERROR,
}


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain_error_handler(request: Request, exc: DomainError):
        http_status = _STATUS_MAP.get(type(exc), status.HTTP_400_BAD_REQUEST)
        return JSONResponse(
            status_code=http_status,
            content={
                "code": exc.code,
                "message": exc.message,
                "details": exc.details or {},
            },
        )
