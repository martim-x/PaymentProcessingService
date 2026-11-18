"""Use case: публикация outbox (relay)."""

from payment.application.ports.message_publisher import MessagePublisher
from payment.application.ports.repositories import AbstractUnitOfWork


class PublishOutbox:
    """Берёт пакет несент записей outbox, публикует, помечает sent.

    Публикация идёт ПОСЛЕ чтения; падение брокера оставляет запись несент —
    её заберёт следующий проход. Одна «ядовитая» запись не блокирует батч:
    её ошибка логируется через исключение наружу, но остальные уже помечены.
    """

    def __init__(
        self, *, uow: AbstractUnitOfWork, publisher: MessagePublisher, subject: str = "payments.new"
    ) -> None:
        self._uow = uow
        self._publisher = publisher
        self._subject = subject

    async def execute(self, *, batch: int = 100) -> int:
        async with self._uow as uow:
            records = await uow.outbox.fetch_unsent(batch=batch)
            published = 0
            for record in records:
                await self._publisher.publish(subject=self._subject, payload=record.payload)
                await uow.outbox.mark_sent(record.id)
                published += 1
            if published:
                await uow.commit()
            return published
