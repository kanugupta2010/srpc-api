"""Phase C — bill_references, allocations, audit_log.

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-18

Per CloudAccountingDesign §7.

bill_references:
    One row is created automatically for every Sales / Purchase Invoice
    (that part comes in Phase E). Credit Note, Debit Note, Receipt, and
    Payment vouchers may allocate against these bills.

    The outstanding_amount column is maintained by the settlement
    service — it is NOT a generated column, because MySQL generated
    columns can't reference other tables (SUM(allocations)). Instead,
    the service layer recomputes it inside the same transaction as
    every allocation insert / allocation-reversal.

allocations:
    Append-only. One row per (allocating_voucher, bill_reference) pair,
    with the enforced uniqueness preventing double-allocation of the
    same voucher to the same bill. To increase an allocation, you
    cancel + repost.

audit_log:
    Append-only log of state-changing events. Phase C writes here
    whenever a voucher is posted or cancelled. Phase F will hook the
    Busy import transformer into it.
"""
from __future__ import annotations

from alembic import op


revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # bill_references
    # ------------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE bill_references (
            id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id         BIGINT UNSIGNED NOT NULL,
            party_ledger_id         BIGINT UNSIGNED NOT NULL,
            bill_no                 VARCHAR(40)     NOT NULL,
                                    -- Usually voucher_number of source invoice
            bill_date               DATE            NOT NULL,
            due_date                DATE            NULL,
            original_amount         DECIMAL(18,2)   NOT NULL,
            outstanding_amount      DECIMAL(18,2)   NOT NULL,
                                    -- Maintained by settlement service.
                                    -- Constraint: 0 <= outstanding_amount
                                    -- <= original_amount.
            source_voucher_id       BIGINT UNSIGNED NULL,
                                    -- NULL for opening-balance bills
                                    -- bootstrapped without a voucher.
            side                    ENUM('RECEIVABLE','PAYABLE')
                                    NOT NULL,
                                    -- RECEIVABLE  — Debtor owes us (sales inv)
                                    -- PAYABLE     — We owe Creditor (purch inv)
            status                  ENUM('OPEN','PARTIAL','CLEARED','WRITTEN_OFF')
                                    NOT NULL DEFAULT 'OPEN',
            notes                   VARCHAR(500)    NULL,
            created_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at              DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_bill_per_party (organization_id, party_ledger_id,
                                          bill_no),
            KEY idx_br_org_party_status
                (organization_id, party_ledger_id, status),
            KEY idx_br_org_status (organization_id, status),
            KEY idx_br_source_voucher (source_voucher_id),

            CONSTRAINT fk_br_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_br_party_ledger
                FOREIGN KEY (party_ledger_id) REFERENCES ledgers (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_br_voucher
                FOREIGN KEY (source_voucher_id) REFERENCES vouchers (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT chk_br_outstanding_range
                CHECK (outstanding_amount >= 0
                       AND outstanding_amount <= original_amount),
            CONSTRAINT chk_br_original_positive
                CHECK (original_amount > 0)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Open bills per party; outstanding maintained by service';
        """
    )

    # ------------------------------------------------------------------
    # allocations
    # ------------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE allocations (
            id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id         BIGINT UNSIGNED NOT NULL,
            allocating_voucher_id   BIGINT UNSIGNED NOT NULL,
                                    -- The Receipt/Payment/CN/DN voucher
            bill_reference_id       BIGINT UNSIGNED NOT NULL,
            amount                  DECIMAL(18,2)   NOT NULL,
            is_reversed             TINYINT(1)      NOT NULL DEFAULT 0,
                                    -- Flipped to 1 when the allocating
                                    -- voucher is cancelled. The row is
                                    -- never deleted — preserves audit
                                    -- trail. Reversed rows do NOT count
                                    -- toward outstanding recomputation.
            reversed_at             DATETIME        NULL,
            allocated_at            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_alloc_voucher_bill
                (allocating_voucher_id, bill_reference_id),
            KEY idx_alloc_org_bill (organization_id, bill_reference_id),
            KEY idx_alloc_org_voucher (organization_id, allocating_voucher_id),

            CONSTRAINT fk_alloc_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_alloc_voucher
                FOREIGN KEY (allocating_voucher_id) REFERENCES vouchers (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT fk_alloc_bill
                FOREIGN KEY (bill_reference_id) REFERENCES bill_references (id)
                ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT chk_alloc_amount_positive
                CHECK (amount > 0)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Allocations of receipts/payments to bills (append-only)';
        """
    )

    # ------------------------------------------------------------------
    # audit_log
    # ------------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE audit_log (
            id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            organization_id     BIGINT UNSIGNED NOT NULL,
            entity_type         VARCHAR(40)     NOT NULL,
                                -- 'voucher', 'allocation', 'party', ...
            entity_id           BIGINT UNSIGNED NULL,
            action              VARCHAR(40)     NOT NULL,
                                -- 'POSTED', 'CANCELLED', 'ALLOCATED',
                                -- 'ALLOCATION_REVERSED', 'CREATED'
            actor               VARCHAR(120)    NULL,
                                -- username / admin name from JWT
            details             JSON            NULL,
                                -- arbitrary structured payload, e.g.
                                -- { "voucher_number": "RV0005",
                                --   "total_amount": "15000.00" }
            created_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            KEY idx_audit_org_entity
                (organization_id, entity_type, entity_id),
            KEY idx_audit_org_created (organization_id, created_at),

            CONSTRAINT fk_audit_org
                FOREIGN KEY (organization_id) REFERENCES organizations (id)
                ON UPDATE CASCADE ON DELETE RESTRICT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='Append-only audit trail; never UPDATE or DELETE rows';
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS audit_log;")
    op.execute("DROP TABLE IF EXISTS allocations;")
    op.execute("DROP TABLE IF EXISTS bill_references;")
