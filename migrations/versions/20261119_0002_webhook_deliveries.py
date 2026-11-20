"""Таблица задач доставки webhook (вариант 2: outbox -> отдельный обработчик).

Revision ID: 0002_webhook_deliveries
Revises: 0001_initial
Create Date: 2026-11-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0002_webhook_deliveries"
down_revision: str | None = "0001_initial"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_deliveries",
        sa.Column("event_id", sa.String(64), primary_key=True, nullable=False),
        sa.Column("payment_id", UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("locked_until", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("delivered_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index("ix_webhook_deliveries_payment_id", "webhook_deliveries", ["payment_id"])
    # relay выбирает записи по (sent, failed) и маршрутизирует по event_type
    op.create_index("ix_outbox_event_type", "outbox", ["event_type"])


def downgrade() -> None:
    op.drop_index("ix_outbox_event_type", table_name="outbox")
    op.drop_index("ix_webhook_deliveries_payment_id", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
