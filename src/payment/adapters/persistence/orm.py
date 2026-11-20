"""SQLAlchemy-таблицы (imperative core, без декларативных моделей).

Доменные объекты маппятся вручную в репозиториях — ядро зависит только от домена.
"""

from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

payments_table = Table(
    "payments",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("amount", Numeric(19, 4), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("description", Text, nullable=False, default="", server_default=""),
    Column("metadata", JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")),
    Column("status", String(16), nullable=False),
    Column("fingerprint", String(64), nullable=True),
    Column("idempotency_key", String(255), nullable=False, unique=True),
    Column("webhook_url", Text, nullable=False),
    Column("failure_reason", Text, nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("processed_at", TIMESTAMP(timezone=True), nullable=True),
    Column("locked_until", TIMESTAMP(timezone=True), nullable=True),
)

outbox_table = Table(
    "outbox",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("aggregate_id", String(64), nullable=False),
    Column("event_type", String(64), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("sent", Boolean, nullable=False, default=False, server_default=text("false")),
    Column("sent_at", TIMESTAMP(timezone=True), nullable=True),
    # 'relay' — публикует relay; 'local' — запись хранится для consumer'а и не
    # подлежит публикации (см. migrations/versions/20261120_0003_outbox_dispatch.py)
    Column("dispatch", String(16), nullable=False, default="relay", server_default=text("'relay'")),
    Column("failed", Boolean, nullable=False, default=False, server_default=text("false")),
    Column("attempts", Integer, nullable=False, default=0, server_default=text("0")),
)

# Задача доставки webhook: один ряд на событие (стабильный event_id из outbox).
# locked_until — клейм, чтобы два consumer'а не отправили один webhook параллельно.
webhook_deliveries_table = Table(
    "webhook_deliveries",
    metadata,
    Column("event_id", String(64), primary_key=True),
    Column("payment_id", UUID(as_uuid=True), nullable=False),
    Column(
        "status", String(16), nullable=False, default="pending", server_default=text("'pending'")
    ),
    Column("attempts", Integer, nullable=False, default=0, server_default=text("0")),
    Column("last_error", Text, nullable=True),
    Column("locked_until", TIMESTAMP(timezone=True), nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
    Column("delivered_at", TIMESTAMP(timezone=True), nullable=True),
)

Index("ix_payments_created_at", payments_table.c.created_at)
Index(
    "ix_outbox_unsent",
    outbox_table.c.created_at,
    postgresql_where=text("sent = false"),
)
Index(
    "ix_outbox_relay_pending",
    outbox_table.c.created_at,
    postgresql_where=text("sent = false AND failed = false AND dispatch = 'relay'"),
)
Index("ix_outbox_event_type", outbox_table.c.event_type)
Index("ix_webhook_deliveries_payment_id", webhook_deliveries_table.c.payment_id)
