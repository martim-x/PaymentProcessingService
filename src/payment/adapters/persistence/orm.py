"""SQLAlchemy-таблицы (imperative core, без декларативных моделей).

Доменные объекты маппятся вручную в репозиториях — ядро зависит только от домена.
"""

from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

payments_table = Table(
    "payments",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("amount", Numeric(19, 4), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("description", Text, nullable=False, default=""),
    Column("metadata", JSONB, nullable=False, default=dict),
    Column("status", String(16), nullable=False),
    Column("fingerprint", String(64), nullable=True),
    Column("idempotency_key", String(255), nullable=False, unique=True),
    Column("webhook_url", Text, nullable=False),
    Column("failure_reason", Text, nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("processed_at", TIMESTAMP(timezone=True), nullable=True),
)

outbox_table = Table(
    "outbox",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("aggregate_id", String(64), nullable=False),
    Column("event_type", String(64), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("sent", Boolean, nullable=False, default=False),
    Column("sent_at", TIMESTAMP(timezone=True), nullable=True),
)
