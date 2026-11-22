"""Репозиторий задач доставки webhook на SQLAlchemy.

Все переходы статуса условны по fencing token (`locked_until`): записать итог
доставки может только владелец действующего клейма. Иначе устаревший consumer,
переживший истечение TTL, перетёр бы состояние нового обработчика и потерял
уведомление (или продлил жизненный цикл мёртвой задачи).
"""

from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar
from uuid import UUID

from sqlalchemy import Row, case, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment.adapters.persistence.orm import webhook_deliveries_table
from payment.application.ports.repositories import (
    WebhookDelivery,
    WebhookDeliveryStatus,
)


class SqlAlchemyWebhookDeliveryRepository:
    """Хранит состояние webhook-доставки и fencing-клеймы в PostgreSQL."""

    _MAX_ERROR_LENGTH: ClassVar[int] = 2000

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _delivery_from_row(row: Row[Any]) -> WebhookDelivery:
        return WebhookDelivery(
            event_id=row.event_id,
            payment_id=row.payment_id,
            status=WebhookDeliveryStatus(row.status),
            attempts=row.attempts,
            last_error=row.last_error,
            locked_until=row.locked_until,
        )

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
        now = self._now()
        await self._session.execute(
            insert(webhook_deliveries_table)
            .values(
                event_id=event_id,
                payment_id=payment_id,
                status=WebhookDeliveryStatus.PENDING.value,
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
        now = self._now()
        until = now + timedelta(seconds=ttl_seconds)
        result = await self._session.execute(
            update(webhook_deliveries_table)
            .where(
                webhook_deliveries_table.c.event_id == event_id,
                webhook_deliveries_table.c.status.in_(
                    [
                        WebhookDeliveryStatus.PENDING.value,
                        WebhookDeliveryStatus.DELIVERING.value,
                    ]
                ),
                or_(
                    webhook_deliveries_table.c.locked_until.is_(None),
                    webhook_deliveries_table.c.locked_until < now,
                ),
            )
            .values(
                status=WebhookDeliveryStatus.DELIVERING.value,
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
                webhook_deliveries_table.c.status
                == WebhookDeliveryStatus.DELIVERING.value,
                webhook_deliveries_table.c.locked_until == claim_token,
            )
            .values(
                status=WebhookDeliveryStatus.PENDING.value,
                locked_until=None,
                updated_at=self._now(),
            )
        )

    async def mark_delivered(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> bool:
        now = self._now()
        result = await self._session.execute(
            update(webhook_deliveries_table)
            .where(
                webhook_deliveries_table.c.event_id == event_id,
                webhook_deliveries_table.c.status
                == WebhookDeliveryStatus.DELIVERING.value,
                webhook_deliveries_table.c.locked_until == claim_token,
            )
            .values(
                status=WebhookDeliveryStatus.DELIVERED.value,
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
        now = self._now()
        attempts = webhook_deliveries_table.c.attempts + 1
        exhausted = attempts >= max_attempts
        result = await self._session.execute(
            update(webhook_deliveries_table)
            .where(
                webhook_deliveries_table.c.event_id == event_id,
                webhook_deliveries_table.c.status
                == WebhookDeliveryStatus.DELIVERING.value,
                webhook_deliveries_table.c.locked_until == claim_token,
            )
            .values(
                attempts=attempts,
                status=case(
                    (
                        exhausted,
                        WebhookDeliveryStatus.FAILED.value,
                    ),
                    else_=WebhookDeliveryStatus.PENDING.value,
                ),
                locked_until=None,
                last_error=error[: self._MAX_ERROR_LENGTH],
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
        return int(row.attempts), row.status == WebhookDeliveryStatus.FAILED.value

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
        return self._delivery_from_row(row) if row is not None else None
