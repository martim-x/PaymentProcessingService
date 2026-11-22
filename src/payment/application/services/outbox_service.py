"""Сервис публикации событий из transactional outbox в RabbitMQ."""

import logging
from collections.abc import Mapping

from payment.application.ports.message_publisher import (
    MessagePublisher,
    PublisherUnavailable,
)
from payment.application.ports.repositories import OutboxRecord
from payment.application.ports.unit_of_work import AbstractUnitOfWork

logger = logging.getLogger(__name__)


class OutboxService:
    """Публикует сохранённые события и помечает результат в outbox."""

    def __init__(
        self,
        *,
        uow: AbstractUnitOfWork,
        publisher: MessagePublisher,
        routes: Mapping[str, str] | None = None,
        subject: str = "payments.new",
        max_attempts: int = 10,
    ) -> None:
        self._uow = uow
        self._publisher = publisher
        self._routes = dict(routes) if routes is not None else {}
        self._subject = subject
        self._max_attempts = max_attempts

    def subject_for(
        self,
        *,
        record: OutboxRecord,
    ) -> str:
        """Выбирает очередь по event_type, иначе использует очередь по умолчанию."""
        event_type = record.event_type or str(record.payload.get("event") or "")
        return self._routes.get(
            event_type,
            self._subject,
        )

    @staticmethod
    def message_id_for(*, record: OutboxRecord) -> str | None:
        """Возвращает стабильный ID сообщения для дедупликации у получателя."""
        event_id = record.payload.get("event_id")
        if (
            isinstance(
                event_id,
                str,
            )
            and event_id
        ):
            return event_id
        return str(record.id) if record.id is not None else None

    async def publish_pending(
        self,
        *,
        batch: int = 100,
    ) -> int:
        """Публикует до batch записей; возвращает число подтверждённых публикаций.

        PublisherUnavailable пробрасывается: временный сбой брокера не тратит
        лимит попыток outbox, relay повторит чтение в следующем цикле.
        """
        async with self._uow as uow:
            records = await uow.outbox.fetch_unsent(batch=batch)
            published = 0
            failed = 0
            for record in records:
                try:
                    await self._publisher.publish(
                        subject=self.subject_for(record=record),
                        payload=record.payload,
                        message_id=self.message_id_for(record=record),
                    )
                except PublisherUnavailable:
                    if published or failed:
                        await uow.commit()
                    logger.warning(
                        "broker unavailable; %d record(s) published, %d left for next pass",
                        published,
                        len(records) - published,
                    )
                    raise
                except Exception:  # noqa: BLE001 — poison-запись не блокирует батч
                    dead = await uow.outbox.mark_attempt_failed(
                        record_id=record.id,
                        max_attempts=self._max_attempts,
                    )
                    failed += 1
                    logger.exception(
                        "outbox record %s publish failed (dead=%s)",
                        record.id,
                        dead,
                    )
                else:
                    # Ошибку БД при mark_sent нельзя классифицировать как отказ publish:
                    # сообщение уже могло попасть в брокер, поэтому пробрасываем её.
                    await uow.outbox.mark_sent(record_id=record.id)
                    published += 1
            if published or failed:
                await uow.commit()
            return published
