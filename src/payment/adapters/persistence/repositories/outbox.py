"""Репозиторий outbox на SQLAlchemy."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment.adapters.persistence.orm import outbox_table
from payment.application.ports.repositories import DISPATCH_RELAY, OutboxRecord


def _record_from_row(row) -> OutboxRecord:
    return OutboxRecord(
        id=row.id,
        aggregate_id=row.aggregate_id,
        payload=dict(row._mapping["payload"]),
        sent=row.sent,
        attempts=row.attempts,
        failed=row.failed,
        event_type=row.event_type,
        dispatch=row.dispatch,
    )


class SqlAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_event(self, *, aggregate_id: str, event_type: str) -> OutboxRecord | None:
        result = await self._session.execute(
            select(outbox_table)
            .where(
                outbox_table.c.aggregate_id == aggregate_id, outbox_table.c.event_type == event_type
            )
            .order_by(outbox_table.c.created_at, outbox_table.c.id)
            .limit(1)
        )
        row = result.first()
        return _record_from_row(row) if row is not None else None

    async def add(
        self, *, payload: dict, aggregate_id: str, dispatch: str = DISPATCH_RELAY
    ) -> OutboxRecord:
        record_id = uuid.uuid4()
        event_type = str(payload.get("event", "unknown"))
        await self._session.execute(
            insert(outbox_table).values(
                id=record_id,
                aggregate_id=aggregate_id,
                event_type=event_type,
                payload=dict(payload),
                created_at=datetime.now(timezone.utc),
                sent=False,
                dispatch=dispatch,
            )
        )
        return OutboxRecord(
            id=record_id,
            aggregate_id=aggregate_id,
            payload=dict(payload),
            event_type=event_type,
            dispatch=dispatch,
        )

    async def fetch_unsent(self, batch: int = 100) -> list[OutboxRecord]:
        """Публикуемые записи: `dispatch='local'` relay не видит вообще."""
        result = await self._session.execute(
            select(outbox_table)
            .where(
                outbox_table.c.sent.is_(False),
                outbox_table.c.failed.is_(False),
                outbox_table.c.dispatch == DISPATCH_RELAY,
            )
            .order_by(outbox_table.c.created_at)
            .limit(batch)
            .with_for_update(skip_locked=True)
        )
        return [_record_from_row(row) for row in result]

    async def mark_sent(self, record_id) -> None:
        await self._session.execute(
            update(outbox_table)
            .where(outbox_table.c.id == record_id)
            .values(sent=True, sent_at=datetime.now(timezone.utc))
        )

    async def mark_attempt_failed(self, record_id, max_attempts: int) -> bool:
        """+1 к attempts; после max_attempts запись помечается failed (dead-letter)."""
        result = await self._session.execute(
            update(outbox_table)
            .where(outbox_table.c.id == record_id)
            .values(
                attempts=outbox_table.c.attempts + 1,
                failed=(outbox_table.c.attempts + 1) >= max_attempts,
            )
            .returning(outbox_table.c.failed)
        )
        row = result.first()
        return bool(row.failed) if row is not None else False
