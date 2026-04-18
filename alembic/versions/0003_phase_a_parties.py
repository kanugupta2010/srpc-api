"""Phase A — parties master (customers + suppliers).

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-18

Per CloudAccountingDesign.docx §4.1, every Party has a corresponding party
ledger under Sundry Debtors (customer) or Sundry Creditors (supplier). The
party_id <-> ledger_id link is bidirectional: parties.ledger_id and
ledgers.party_id. This migration creates the parties table; the FK back from
ledgers.party_id was already created in 0002 with no FK constraint (because
parties did not exist yet) — we add the FK now.
"""
from __future__ import annotations

from alembic import op


revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE parties (
            id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id     BIGINT UNSIGNED NOT NULL,
            party_type          ENUM('CUSTOMER','SUPPLIER','BOTH')
                                NOT NULL DEFAULT 'CUSTOMER',
            name                VARCHAR(200)    NOT NULL,
            display_name        VARCHAR(200)    NULL,
            mobile              VARCHAR(20)     NULL,
            email               VARCHAR(120)    NULL,
            gstin               VARCHAR(15)     NULL,
            pan                 VARCHAR(10)     NULL,
            state_code          VARCHAR(2)      NULL,
            address_line1       VARCHAR(200)    NULL,
            address_line2       VARCHAR(200)    NULL,
            city                VARCHAR(80)     NULL,
            pincode             VARCHAR(10)     NULL,
            credit_limit        DECIMAL(18,2)   NULL,
            credit_days         INT             NULL,
            -- Bidirectional link to the party's auto-created ledger.
            -- Filled in by the seed/posting service the moment the ledger
            -- is created; nullable only for the brief transactional window
            -- inside that service.
            ledger_id           BIGINT UNSIGNED NULL,
            is_active           TINYINT(1)      NOT NULL DEFAULT 1,
            created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_party_name_per_org (organization_id, party_type, name),
            KEY idx_party_org (organization_id),
            KEY idx_party_mobile (mobile),
            KEY idx_party_gstin (gstin),
            KEY idx_party_ledger (ledger_id),

            CONSTRAINT fk_party_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_party_ledger
                FOREIGN KEY (ledger_id) REFERENCES ledgers (id)
                ON UPDATE CASCADE ON DELETE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Customers and suppliers; each gets an auto-created party ledger';
        """
    )

    # Now add the reverse FK on ledgers.party_id, which we deferred in 0002.
    op.execute(
        """
        ALTER TABLE ledgers
            ADD CONSTRAINT fk_ledger_party
                FOREIGN KEY (party_id) REFERENCES parties (id)
                ON UPDATE CASCADE ON DELETE RESTRICT;
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE ledgers DROP FOREIGN KEY fk_ledger_party;")
    op.execute("DROP TABLE IF EXISTS parties;")
