"""Baseline — existing schema is assumed to be already applied.

Revision ID: 0001
Revises:
Create Date: 2026-04-18

This migration intentionally does NOTHING. The live MySQL database already has
the loyalty schema (settings, item_master, contractors, invoices, invoice_lines,
purchase_invoices, purchase_lines, points_log, redemptions, otp_sessions,
import_batches, item_tags, item_tag_map, companies, etc.) created by
srpc_loyalty_schema.sql.

We stamp this revision so future migrations stack cleanly:

    alembic stamp 0001                 # tells Alembic the live DB is at 0001
    alembic upgrade head               # applies 0002 onwards

If you ever provision a fresh database, run srpc_loyalty_schema.sql FIRST,
then `alembic stamp 0001`, then `alembic upgrade head`.
"""
from __future__ import annotations

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
