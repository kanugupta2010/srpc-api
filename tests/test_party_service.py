"""Tests for core.services.party_service."""
from __future__ import annotations

from decimal import Decimal

import pytest

from core.db import tx
from core.services.party_service import (
    CreatePartyInput,
    PartyDuplicate,
    create_party,
)
from tests.conftest import D, TEST_ORG_ID


def test_create_customer_creates_ledger_under_sundry_debtors(bound, coa):
    """Creating a CUSTOMER party auto-creates a ledger under Sundry Debtors."""
    # conftest's `coa` fixture creates Sundry Debtors group. But party_service
    # also looks up reserved groups by name — ensure the fixture's naming
    # works. If the test DB doesn't have Sundry Debtors, this test confirms
    # the error path clearly.
    result = create_party(CreatePartyInput(
        party_type="CUSTOMER",
        name="Test Customer Pvt Ltd",
        mobile="9999999999",
        gstin="07ABCDE1234F1Z5",
        state_code="07",
    ))

    assert result.party_id > 0
    assert result.ledger_id > 0
    assert result.opening_voucher_id is None

    # Verify the ledger is under Sundry Debtors.
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT l.id, l.is_party, l.party_id, g.name AS group_name "
                "FROM ledgers l "
                "JOIN account_groups g "
                "       ON g.id = l.group_id "
                "      AND g.organization_id = l.organization_id "
                "WHERE l.organization_id = %s AND l.id = %s",
                (TEST_ORG_ID, result.ledger_id),
            )
            row = cur.fetchone()
        finally:
            cur.close()

    assert row["group_name"] == "Sundry Debtors"
    assert row["is_party"] == 1
    assert row["party_id"] == result.party_id


def test_create_supplier_creates_ledger_under_sundry_creditors(bound, coa):
    """Creating a SUPPLIER creates a ledger under Sundry Creditors."""
    # conftest's coa fixture doesn't create Sundry Creditors by default,
    # so we add it here before the party creation.
    with tx() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM account_groups "
                "WHERE organization_id = %s AND name = 'Current Liabilities'",
                (TEST_ORG_ID,),
            )
            row = cur.fetchone()
            if row:
                cl_id = int(row[0])
            else:
                cur.execute(
                    "INSERT INTO account_groups "
                    "(organization_id, parent_group_id, name, nature, "
                    " affects_gross_profit, is_reserved) "
                    "VALUES (%s, NULL, 'Current Liabilities', 'LIABILITY', 0, 1)",
                    (TEST_ORG_ID,),
                )
                cl_id = int(cur.lastrowid)

            cur.execute(
                "INSERT INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, %s, 'Sundry Creditors', 'LIABILITY', 0, 1)",
                (TEST_ORG_ID, cl_id),
            )
        finally:
            cur.close()

    result = create_party(CreatePartyInput(
        party_type="SUPPLIER",
        name="Test Supplier Co",
    ))

    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT g.name AS group_name "
                "FROM ledgers l "
                "JOIN account_groups g "
                "       ON g.id = l.group_id "
                "      AND g.organization_id = l.organization_id "
                "WHERE l.organization_id = %s AND l.id = %s",
                (TEST_ORG_ID, result.ledger_id),
            )
            row = cur.fetchone()
        finally:
            cur.close()
    assert row["group_name"] == "Sundry Creditors"


def test_opening_balance_auto_posts_journal(bound, coa):
    """Party with non-zero opening_balance auto-posts a JOURNAL voucher."""
    # Ensure Capital Account group exists for Opening Balance Equity ledger.
    with tx() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT IGNORE INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, NULL, 'Capital Account', 'EQUITY', 0, 1)",
                (TEST_ORG_ID,),
            )
        finally:
            cur.close()

    result = create_party(CreatePartyInput(
        party_type="CUSTOMER",
        name="Customer With OB",
        opening_balance=D("5000"),
        opening_balance_dr_cr="Dr",
    ))

    assert result.opening_voucher_id is not None

    # The voucher should have 2 balanced lines.
    with tx() as conn:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT voucher_type, total_amount, status "
                "FROM vouchers "
                "WHERE organization_id = %s AND id = %s",
                (TEST_ORG_ID, result.opening_voucher_id),
            )
            vrow = cur.fetchone()

            cur.execute(
                "SELECT ledger_id, dr_cr, amount "
                "FROM voucher_lines "
                "WHERE organization_id = %s AND voucher_id = %s "
                "ORDER BY line_order",
                (TEST_ORG_ID, result.opening_voucher_id),
            )
            lines = cur.fetchall()
        finally:
            cur.close()

    assert vrow["voucher_type"] == "JOURNAL"
    assert vrow["total_amount"] == D("5000.00")
    assert vrow["status"] == "POSTED"
    assert len(lines) == 2

    # Dr hits the new party ledger; Cr hits Opening Balance Equity.
    party_line = next(ln for ln in lines if int(ln["ledger_id"]) == result.ledger_id)
    other_line = next(ln for ln in lines if int(ln["ledger_id"]) != result.ledger_id)
    assert party_line["dr_cr"] == "Dr"
    assert other_line["dr_cr"] == "Cr"
    assert Decimal(party_line["amount"]) == D("5000.00")


def test_duplicate_party_name_rejected(bound, coa):
    create_party(CreatePartyInput(
        party_type="CUSTOMER",
        name="Unique Name Co",
    ))
    with pytest.raises(PartyDuplicate):
        create_party(CreatePartyInput(
            party_type="CUSTOMER",
            name="Unique Name Co",
        ))


def test_same_name_different_party_type_allowed(bound, coa):
    """Same name can exist as both CUSTOMER and SUPPLIER — unique constraint
    is (org, party_type, name)."""
    # Ensure Sundry Creditors exists.
    with tx() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT IGNORE INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, NULL, 'Current Liabilities', 'LIABILITY', 0, 1)",
                (TEST_ORG_ID,),
            )
            cur.execute(
                "SELECT id FROM account_groups "
                "WHERE organization_id = %s AND name = 'Current Liabilities'",
                (TEST_ORG_ID,),
            )
            cl_id = int(cur.fetchone()[0])
            cur.execute(
                "INSERT IGNORE INTO account_groups "
                "(organization_id, parent_group_id, name, nature, "
                " affects_gross_profit, is_reserved) "
                "VALUES (%s, %s, 'Sundry Creditors', 'LIABILITY', 0, 1)",
                (TEST_ORG_ID, cl_id),
            )
        finally:
            cur.close()

    r1 = create_party(CreatePartyInput(
        party_type="CUSTOMER",
        name="Dual Role Co",
    ))
    r2 = create_party(CreatePartyInput(
        party_type="SUPPLIER",
        name="Dual Role Co",
    ))
    assert r1.party_id != r2.party_id
    assert r1.ledger_id != r2.ledger_id
