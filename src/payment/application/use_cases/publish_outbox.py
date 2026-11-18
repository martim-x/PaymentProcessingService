"""Use case: публикация outbox (relay)."""

import logging

from payment.application.ports.message_publisher import MessagePublisher
from payment.application.ports.repositories import AbstractUnitOfWork

logger = logging.getLogger(__name__)


class PublishOutbox:
    """Берёт пакет несент записей outbox, публикует, помечает sent.

    - ошибка на одной записи НЕ откатывает остальные: каждая запись
      обрабатывается отдельно, сбойная получает +1 к attempts;
    - после max_attempts сбойная запись помечается failed (dead-letter
      на уровне outbox) и больше не блокирует очередь;
    - если лёг брокер, все записи батча получат +1 к attempts — это
      ожидаемый ретрай на следующем проходе, поэтому max_attempts должен
      быть заметно больше, чем «сколько раз подряд может лежать брокер».
    """

    def __init__(
        self,
        *,
        uow: AbstractUnitOfWork,
        publisher: MessagePublisher,
        subject: str = "payments.new",
        max_attempts: int = 10,
    ) -> None:
        self._uow = uow
        self._publisher = publisher
        self._subject = subject
        self._max_attempts = max_attempts

    async def execute(self, *, batch: int = 100) -> int:
        async with self._uow as uow:
            records = await uow.outbox.fetch_unsent(batch=batch)
            published = 0
            failed = 0
            for record in records:
                try:
                    await self._publisher.publish(subject=self._subject, payload=record.payload)
                    await uow.outbox.mark_sent(record.id)
                    published += 1
                except Exception:  # noqa: BLE001 — одна запись не должна ронять батч
                    dead = await uow.outbox.mark_attempt_failed(record.id, self._max_attempts)
                    failed += 1
                    logger.exception("outbox record %s publish failed (dead=%s)", record.id, dead)
            if published or failed:
                await uow.commit()
            return published
