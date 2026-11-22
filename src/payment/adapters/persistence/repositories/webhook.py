"""Репозиторий задач доставки webhook на SQLAlchemy.

Все переходы статуса условны по fencing token (`locked_until`): записать итог
доставки может только владелец действующего клейма. Иначе устаревший consumer,
переживший истечение TTL, перетёр бы состояние нового обработчика и потерял
уведомление (или продлил жизненный цикл мёртвой задачи).
"""

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import Row, case, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment.adapters.persistence.orm import webhook_deliveries_table
from payment.application.ports.repositories import (
    DELIVERY_DELIVERED,
    DELIVERY_DELIVERING,
    DELIVERY_FAILED,
    DELIVERY_PENDING,
    WebhookDelivery,
)

_MAX_ERROR_LENGTH = 2000


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _delivery_from_row(row: Row[Any]) -> WebhookDelivery:
    return WebhookDelivery(
        event_id=row.event_id,
        payment_id=row.payment_id,
        status=row.status,
        attempts=row.attempts,
        last_error=row.last_error,
        locked_until=row.locked_until,
    )


class SqlAlchemyWebhookDeliveryRepository:
    """Хранит состояние webhook-доставки и fencing-клеймы в PostgreSQL."""

    def __init__(
        self,
        *,
        session: AsyncSession,
    ) -> None:
        self._session = session

    async def create_if_absent(
        self,
        *,
        event_id: str,
        payment_id: UUID,
    ) -> WebhookDelivery:
        now = _now()
        await self._session.execute(
            insert(webhook_deliveries_table)
            .values(
                event_id=event_id,
                payment_id=payment_id,
                status=DELIVERY_PENDING,
                attempts=0,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=["event_id"])
        )
        record = await self.get(event_id=event_id)
        if record is None:  # pragma: no cover — гонка create/delete невозможна
            raise RuntimeError(f"webhook delivery {event_id} vanished after upsert")
        return record

    async def try_claim(
        self,
        *,
        event_id: str,
        ttl_seconds: float,
    ) -> datetime | None:
        now = _now()
        until = now + timedelta(seconds=ttl_seconds)
        result = await self._session.execute(
            update(webhook_deliveries_table)
            .where(
                webhook_deliveries_table.c.event_id == event_id,
                webhook_deliveries_table.c.status.in_(
                    [
                        DELIVERY_PENDING,
                        DELIVERY_DELIVERING,
                    ]
                ),
                or_(
                    webhook_deliveries_table.c.locked_until.is_(None),
                    webhook_deliveries_table.c.locked_until < now,
                ),
            )
            .values(
                status=DELIVERY_DELIVERING,
                locked_until=until,
                updated_at=now,
            )
            .returning(webhook_deliveries_table.c.locked_until)
        )
        row = result.first()
        return row.locked_until if row is not None else None

    async def release_claim(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> None:
        """Снять клейм может только его владелец: чужой клейм не трогаем."""
        await self._session.execute(
            update(webhook_deliveries_table)
            .where(
                webhook_deliveries_table.c.event_id == event_id,
                webhook_deliveries_table.c.status == DELIVERY_DELIVERING,
                webhook_deliveries_table.c.locked_until == claim_token,
            )
            .values(
                status=DELIVERY_PENDING,
                locked_until=None,
                updated_at=_now(),
            )
        )

    async def mark_delivered(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> bool:
        now = _now()
        result = await self._session.execute(
            update(webhook_deliveries_table)
            .where(
                webhook_deliveries_table.c.event_id == event_id,
                webhook_deliveries_table.c.status == DELIVERY_DELIVERING,
                webhook_deliveries_table.c.locked_until == claim_token,
            )
            .values(
                status=DELIVERY_DELIVERED,
                locked_until=None,
                last_error=None,
                delivered_at=now,
                updated_at=now,
            )
            .returning(webhook_deliveries_table.c.event_id)
        )
        return result.first() is not None

    async def mark_attempt_failed(
        self,
        *,
        event_id: str,
        claim_token: datetime,
        max_attempts: int,
        error: str,
    ) -> tuple[int, bool] | None:
        now = _now()
        attempts = webhook_deliveries_table.c.attempts + 1
        exhausted = attempts >= max_attempts
        result = await self._session.execute(
            update(webhook_deliveries_table)
            .where(
                webhook_deliveries_table.c.event_id == event_id,
                webhook_deliveries_table.c.status == DELIVERY_DELIVERING,
                webhook_deliveries_table.c.locked_until == claim_token,
            )
            .values(
                attempts=attempts,
                status=case(
                    (
                        exhausted,
                        DELIVERY_FAILED,
                    ),
                    else_=DELIVERY_PENDING,
                ),
                locked_until=None,
                last_error=error[:_MAX_ERROR_LENGTH],
                updated_at=now,
            )
            .returning(
                webhook_deliveries_table.c.attempts,
                webhook_deliveries_table.c.status,
            )
        )
        row = result.first()
        if row is None:
            # клейм устарел: попытку засчитает и состояние запишет новый владелец
            return None
        return int(row.attempts), row.status == DELIVERY_FAILED

    async def get(
        self,
        *,
        event_id: str,
    ) -> WebhookDelivery | None:
        result = await self._session.execute(
            select(webhook_deliveries_table).where(
                webhook_deliveries_table.c.event_id == event_id
            )
        )
        row = result.first()
        return _delivery_from_row(row) if row is not None else None
