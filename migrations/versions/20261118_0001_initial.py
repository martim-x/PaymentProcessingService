"""Начальная схема: payments и outbox.

Revision ID: 0001_initial
Revises:
Create Date: 2026-11-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "amount",
            sa.Numeric(
                19,
                4,
            ),
            nullable=False,
        ),
        sa.Column(
            "currency",
            sa.String(3),
            nullable=False,
        ),
        sa.Column(
            "description",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        sa.Column(
            "metadata",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "status",
            sa.String(16),
            nullable=False,
        ),
        sa.Column(
            "fingerprint",
            sa.String(64),
            nullable=True,
        ),
        sa.Column(
            "idempotency_key",
            sa.String(255),
            nullable=False,
        ),
        sa.Column(
            "webhook_url",
            sa.Text(),
            nullable=False,
        ),
        sa.Column(
            "failure_reason",
            sa.Text(),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "processed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "locked_until",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_payments_idempotency_key",
        ),
    )
    op.create_index(
        "ix_payments_created_at",
        "payments",
        ["created_at"],
    )

    op.create_table(
        "outbox",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "aggregate_id",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "event_type",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "payload",
            JSONB(),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "sent",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "sent_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "failed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.create_index(
        "ix_outbox_unsent",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("sent = false"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_unsent",
        table_name="outbox",
    )
    op.drop_table("outbox")
    op.drop_index(
        "ix_payments_created_at",
        table_name="payments",
    )
    op.drop_table("payments")
