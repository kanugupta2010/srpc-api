"""Domain-specific exceptions for the accounting core.

These map 1:1 to the stable error codes documented in
CloudAccountingDesign.docx §17.3. The HTTP layer translates them into
{ code, message, details[] } envelopes — never let raw exceptions escape.
"""
from __future__ import annotations


class DomainError(Exception):
    """Base class for all accounting-core domain errors."""

    code: str = "DOMAIN_ERROR"

    def __init__(self, message: str, **details: object) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class VoucherUnbalancedError(DomainError):
    code = "VOUCHER_UNBALANCED"


class VoucherPeriodLockedError(DomainError):
    code = "VOUCHER_PERIOD_LOCKED"


class VoucherDuplicateNumberError(DomainError):
    code = "VOUCHER_DUPLICATE_NUMBER"


class VoucherTypeRuleViolation(DomainError):
    code = "VOUCHER_TYPE_RULE_VIOLATION"


class VoucherNotPostedError(DomainError):
    code = "VOUCHER_NOT_POSTED"


class VoucherAlreadyCancelledError(DomainError):
    code = "VOUCHER_ALREADY_CANCELLED"


class VoucherImmutableError(DomainError):
    """Attempt to mutate a POSTED voucher."""

    code = "VOUCHER_IMMUTABLE"


class CrossOrgReferenceError(DomainError):
    """A line referenced a ledger from a different organization."""

    code = "CROSS_ORG_REFERENCE"


class LedgerInactiveError(DomainError):
    code = "LEDGER_INACTIVE"


class FinancialYearNotFoundError(DomainError):
    code = "FINANCIAL_YEAR_NOT_FOUND"


class VoucherSeriesNotFoundError(DomainError):
    code = "VOUCHER_SERIES_NOT_FOUND"


class TenancyContextMissingError(DomainError):
    """A query reached the DB layer without organization_id bound."""

    code = "TENANCY_CONTEXT_MISSING"
