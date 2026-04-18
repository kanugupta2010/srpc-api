"""Phase B — vouchers + voucher_lines (double-entry core).

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-18

Per CloudAccountingDesign.docx §4.2. The voucher is the atomic unit of
posting; voucher_lines are the Dr/Cr entries that must balance to the paisa
before the voucher transitions DRAFT -> POSTED.

The schema enforces what it can declaratively:
- amount must be > 0 (sign comes from dr_cr)
- (organization_id, voucher_type, financial_year_id, voucher_number) UNIQUE
- FKs everywhere

The balanced-debits invariant is enforced in the service layer
(core/services/posting_service.py) inside a single SQL transaction.
A trigger-based invariant was considered and rejected because it complicates
the cancel/reverse flow and the reversing-entry pattern.

The status lifecycle is DRAFT -> POSTED -> CANCELLED.
- DRAFT: row exists, lines may not balance, can be edited.
- POSTED: lines balance, cannot be edited; cancellable only.
- CANCELLED: original is preserved, a new voucher with source_doc_type =
  'REVERSAL_OF' carries the reversing entries. Both rows are kept forever.
"""
from __future__ import annotations

from alembic import op


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE vouchers (
            id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id     BIGINT UNSIGNED NOT NULL,
            voucher_type        ENUM('PAYMENT','RECEIPT','JOURNAL','CONTRA',
                                     'SALES','PURCHASE','CREDIT_NOTE',
                                     'DEBIT_NOTE','STOCK_JOURNAL')
                                NOT NULL,
            voucher_series_id   BIGINT UNSIGNED NOT NULL,
            financial_year_id   BIGINT UNSIGNED NOT NULL,
            voucher_number      VARCHAR(40)     NOT NULL,
            voucher_date        DATE            NOT NULL,
            reference_no        VARCHAR(60)     NULL,
                                -- External reference: supplier inv no, cheque no
            party_ledger_id     BIGINT UNSIGNED NULL,
                                -- The party ledger this voucher relates to
                                -- (debtor for sales/receipt, creditor for
                                -- purchase/payment). NULL for journal/contra.
            narration           TEXT            NULL,
            total_amount        DECIMAL(18,2)   NOT NULL DEFAULT 0.00,
                                -- Equals SUM(Dr lines) = SUM(Cr lines).
                                -- Set by service layer at post time.
            status              ENUM('DRAFT','POSTED','CANCELLED')
                                NOT NULL DEFAULT 'DRAFT',
            posted_at           DATETIME        NULL,
            cancelled_at        DATETIME        NULL,
            -- Back-reference to the source business document (sales invoice,
            -- purchase invoice, GRN, etc.). source_doc_type uses VARCHAR
            -- rather than ENUM so future doc types don't require a schema
            -- change. 'REVERSAL_OF' is reserved for cancellation reversers.
            source_doc_type     VARCHAR(40)     NULL,
            source_doc_id       BIGINT UNSIGNED NULL,
            created_by          VARCHAR(120)    NULL,
            created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_voucher_number (organization_id, voucher_type,
                                          financial_year_id, voucher_number),
            KEY idx_voucher_org_date (organization_id, voucher_date),
            KEY idx_voucher_party (organization_id, party_ledger_id),
            KEY idx_voucher_source (source_doc_type, source_doc_id),
            KEY idx_voucher_status (organization_id, status),

            CONSTRAINT fk_voucher_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_voucher_series
                FOREIGN KEY (voucher_series_id) REFERENCES voucher_series (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_voucher_fy
                FOREIGN KEY (financial_year_id) REFERENCES financial_years (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_voucher_party_ledger
                FOREIGN KEY (party_ledger_id) REFERENCES ledgers (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT chk_voucher_total_nonneg
                CHECK (total_amount >= 0)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Voucher header. Posted vouchers are immutable (cancel + reverse).';
        """
    )

    op.execute(
        """
        CREATE TABLE voucher_lines (
            id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id     BIGINT UNSIGNED NOT NULL,
            voucher_id          BIGINT UNSIGNED NOT NULL,
            ledger_id           BIGINT UNSIGNED NOT NULL,
            dr_cr               ENUM('Dr','Cr') NOT NULL,
            amount              DECIMAL(18,2)   NOT NULL,
                                -- Always positive; sign is given by dr_cr.
                                -- Service layer enforces amount > 0.
            cost_center_id      BIGINT UNSIGNED NULL,
                                -- Phase D will introduce cost_centers table;
                                -- column is here now to avoid a later ALTER.
            line_narration      VARCHAR(255)    NULL,
            line_order          INT             NOT NULL DEFAULT 0,
            created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            KEY idx_vl_voucher (voucher_id),
            KEY idx_vl_ledger (organization_id, ledger_id),
            KEY idx_vl_org_voucher (organization_id, voucher_id),

            CONSTRAINT fk_vl_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_vl_voucher
                FOREIGN KEY (voucher_id) REFERENCES vouchers (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            CONSTRAINT fk_vl_ledger
                FOREIGN KEY (ledger_id) REFERENCES ledgers (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT chk_vl_amount_positive
                CHECK (amount > 0)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Dr/Cr lines. SUM(Dr) = SUM(Cr) per voucher, enforced in service.';
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS voucher_lines;")
    op.execute("DROP TABLE IF EXISTS vouchers;")
