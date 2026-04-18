"""Phase A — financial_years + voucher_series.

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-18

financial_years define accounting periods. A period can be locked, after which
no voucher_date inside the period may be posted. Year code follows the existing
'2526' convention (FY 2025-26 in India).

voucher_series provides per-(org, type, FY) numbering with explicit
prefix/format. The next_number column is incremented inside a SELECT ... FOR
UPDATE in the posting service to serialize concurrent posts. The unique
constraint on (org, type, fy, voucher_number) on the vouchers table (created
in 0005) is the backstop.
"""
from __future__ import annotations

from alembic import op


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE financial_years (
            id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id BIGINT UNSIGNED NOT NULL,
            code            VARCHAR(8)      NOT NULL,    -- e.g. '2526'
            start_date      DATE            NOT NULL,
            end_date        DATE            NOT NULL,
            is_locked       TINYINT(1)      NOT NULL DEFAULT 0,
            locked_at       DATETIME        NULL,
            locked_by       VARCHAR(120)    NULL,
            created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_fy_code_per_org (organization_id, code),
            KEY idx_fy_org_dates (organization_id, start_date, end_date),

            CONSTRAINT fk_fy_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Accounting periods. Locked periods reject new postings.';
        """
    )

    op.execute(
        """
        CREATE TABLE voucher_series (
            id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id     BIGINT UNSIGNED NOT NULL,
            financial_year_id   BIGINT UNSIGNED NOT NULL,
            voucher_type        ENUM('PAYMENT','RECEIPT','JOURNAL','CONTRA',
                                     'SALES','PURCHASE','CREDIT_NOTE',
                                     'DEBIT_NOTE','STOCK_JOURNAL')
                                NOT NULL,
            name                VARCHAR(60)     NOT NULL,
                                -- e.g. 'Default', 'Counter-1'; supports
                                -- per-counter or per-godown numbering later.
            prefix              VARCHAR(20)     NOT NULL DEFAULT '',
            suffix              VARCHAR(20)     NOT NULL DEFAULT '',
            -- Width-padded numeric body. Final voucher_number =
            --   prefix || zfill(next_number, padding) || suffix
            padding             TINYINT UNSIGNED NOT NULL DEFAULT 4,
            next_number         BIGINT UNSIGNED NOT NULL DEFAULT 1,
            is_active           TINYINT(1)      NOT NULL DEFAULT 1,
            created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_series (organization_id, financial_year_id,
                                  voucher_type, name),

            CONSTRAINT fk_series_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_series_fy
                FOREIGN KEY (financial_year_id) REFERENCES financial_years (id)
                ON UPDATE CASCADE ON DELETE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Per-(org, FY, type) numbering. Locked via SELECT...FOR UPDATE.';
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS voucher_series;")
    op.execute("DROP TABLE IF EXISTS financial_years;")
