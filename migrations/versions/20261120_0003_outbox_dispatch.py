"""Тип записи outbox: кто её публикует.

Запись `payment.webhook` больше не выдаётся за отправленную: у неё явный
dispatch='local', relay такие строки не читает вообще, а `sent` остаётся
честным флагом публикации. Раньше finalize сразу ставил sent=true, чтобы relay
не открыл второй путь доставки, — из-за этого поле означало «не публиковать», а
не «отправлено».

Revision ID: 0003_outbox_dispatch
Revises: 0002_webhook_deliveries
Create Date: 2026-11-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_outbox_dispatch"
down_revision: str | None = "0002_webhook_deliveries"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox",
        sa.Column(
            "dispatch",
            sa.String(16),
            nullable=False,
            server_default=sa.text("'relay'"),
        ),
    )
    # relay выбирает только публикуемые записи: частичный индекс под этот запрос
    op.create_index(
        "ix_outbox_relay_pending",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text(
            "sent = false AND failed = false AND dispatch = 'relay'"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_relay_pending",
        table_name="outbox",
    )
    op.drop_column(
        "outbox",
        "dispatch",
    )
