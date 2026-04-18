"""Phase A — organizations, account_groups, ledgers; backfill org_id on existing tables.

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-18

Creates the multi-tenancy root and the chart-of-accounts skeleton per
CloudAccountingDesign.docx §3 and §4.1. Backfills organization_id = 1 onto
every existing tenant-scoped table so we can enforce NOT NULL going forward
without touching the running loyalty program.

Tables created:
    organizations
    account_groups       (tree, self-referencing)
    ledgers              (leaves)

Existing tables altered to add organization_id NOT NULL DEFAULT 1:
    item_master, contractors, invoices, invoice_lines,
    purchase_invoices, purchase_lines, points_log, redemptions,
    otp_sessions, import_batches, item_tags, item_tag_map

The DEFAULT 1 stays on the column for now — it's removed in a later
migration once the application code reliably supplies organization_id
on every INSERT. This avoids breaking the live import pipeline today.
"""
from __future__ import annotations

from alembic import op


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


# Tables that exist today and are tenant-scoped. Order matters only
# for cosmetic reasons (logical grouping in the migration log).
EXISTING_TENANT_TABLES = [
    "item_master",
    "item_tags",
    "item_tag_map",
    "contractors",
    "invoices",
    "invoice_lines",
    "purchase_invoices",
    "purchase_lines",
    "points_log",
    "redemptions",
    "otp_sessions",
    "import_batches",
]


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. organizations — the tenant root
    # ------------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE organizations (
            id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            code            VARCHAR(40)     NOT NULL,
            legal_name      VARCHAR(255)    NOT NULL,
            display_name    VARCHAR(255)    NOT NULL,
            gstin           VARCHAR(15)     NULL,
            state_code      VARCHAR(2)      NULL,
            base_currency   CHAR(3)         NOT NULL DEFAULT 'INR',
            is_active       TINYINT(1)      NOT NULL DEFAULT 1,
            created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_org_code (code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Tenant root. Every tenant-scoped table FKs to this.';
        """
    )

    # Insert the SRPC org as id=1 so existing data backfills cleanly.
    op.execute(
        """
        INSERT INTO organizations (id, code, legal_name, display_name, state_code)
        VALUES (1, 'SRPC',
                'SRPC Enterprises Private Limited',
                'Saraswati Hardware, Paints & Sanitary',
                '07');
        """
    )

    # ------------------------------------------------------------------
    # 2. account_groups — COA tree
    # ------------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE account_groups (
            id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id         BIGINT UNSIGNED NOT NULL,
            parent_group_id         BIGINT UNSIGNED NULL,
            name                    VARCHAR(120)    NOT NULL,
            nature                  ENUM('ASSET','LIABILITY','INCOME','EXPENSE','EQUITY')
                                    NOT NULL,
            affects_gross_profit    TINYINT(1)      NOT NULL DEFAULT 0,
            is_reserved             TINYINT(1)      NOT NULL DEFAULT 0,
            is_active               TINYINT(1)      NOT NULL DEFAULT 1,
            created_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_group_name_per_parent (organization_id, parent_group_id, name),
            KEY idx_group_org (organization_id),
            KEY idx_group_parent (parent_group_id),

            CONSTRAINT fk_group_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_group_parent
                FOREIGN KEY (parent_group_id) REFERENCES account_groups (id)
                ON UPDATE CASCADE ON DELETE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Chart of accounts — internal nodes of the COA tree';
        """
    )

    # ------------------------------------------------------------------
    # 3. ledgers — COA leaves
    # ------------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE ledgers (
            id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id         BIGINT UNSIGNED NOT NULL,
            group_id                BIGINT UNSIGNED NOT NULL,
            name                    VARCHAR(120)    NOT NULL,
            opening_balance         DECIMAL(18,2)   NOT NULL DEFAULT 0.00,
            opening_balance_date    DATE            NULL,
            is_party                TINYINT(1)      NOT NULL DEFAULT 0,
            party_id                BIGINT UNSIGNED NULL,
            gstin                   VARCHAR(15)     NULL,
            is_bank                 TINYINT(1)      NOT NULL DEFAULT 0,
            bank_account_no         VARCHAR(40)     NULL,
            bank_ifsc               VARCHAR(15)     NULL,
            bank_branch             VARCHAR(120)    NULL,
            is_reserved             TINYINT(1)      NOT NULL DEFAULT 0,
            is_active               TINYINT(1)      NOT NULL DEFAULT 1,
            created_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_ledger_name_per_org (organization_id, name),
            KEY idx_ledger_org (organization_id),
            KEY idx_ledger_group (group_id),
            KEY idx_ledger_party (party_id),

            CONSTRAINT fk_ledger_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_ledger_group
                FOREIGN KEY (group_id) REFERENCES account_groups (id)
                ON UPDATE CASCADE ON DELETE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Chart of accounts — leaves; the actual posting accounts';
        """
    )

    # ------------------------------------------------------------------
    # 4. Backfill organization_id on every existing tenant-scoped table.
    # Uses DEFAULT 1 so the ALTER and the backfill happen in one statement.
    # The default is intentionally LEFT IN PLACE for now (see migration 00xx
    # later in Phase B/C, after all writers are updated).
    # ------------------------------------------------------------------
    for table in EXISTING_TENANT_TABLES:
        op.execute(
            f"""
            ALTER TABLE {table}
                ADD COLUMN organization_id BIGINT UNSIGNED NOT NULL DEFAULT 1
                AFTER id,
                ADD KEY idx_{table[:30]}_org (organization_id),
                ADD CONSTRAINT fk_{table[:30]}_org
                    FOREIGN KEY (organization_id) REFERENCES organizations (id)
                    ON UPDATE CASCADE ON DELETE RESTRICT;
            """
        )


def downgrade() -> None:
    # Strip FKs and columns in reverse order.
    for table in reversed(EXISTING_TENANT_TABLES):
        op.execute(f"ALTER TABLE {table} DROP FOREIGN KEY fk_{table[:30]}_org;")
        op.execute(f"ALTER TABLE {table} DROP KEY idx_{table[:30]}_org;")
        op.execute(f"ALTER TABLE {table} DROP COLUMN organization_id;")

    op.execute("DROP TABLE IF EXISTS ledgers;")
    op.execute("DROP TABLE IF EXISTS account_groups;")
    op.execute("DROP TABLE IF EXISTS organizations;")
