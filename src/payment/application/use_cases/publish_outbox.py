"""Use case: публикация outbox (relay)."""

from payment.application.dto import OutboxMessage
from payment.application.ports.message_publisher import MessagePublisher
from payment.application.ports.repositories import AbstractUnitOfWork


class PublishOutbox:
    """Берёт пакет несент записей outbox, публикует, помечает sent.

    Публикация идёт ПОСЛЕ чтения; падение брокера оставляет записи несент —
    их заберёт следующий проход. Задержку между попытками даёт интервал relay.
    """

    def __init__(self, *, uow: AbstractUnitOfWork, publisher: MessagePublisher) -> None:
        self._uow = uow
        self._publisher = publisher
        self._subject = "payments.new"

    async def execute(self, *, batch: int = 100) -> int:
        async with self._uow as uow:
            records = await uow.outbox.fetch_unsent(batch=batch)
            published = 0
            for record in records:
                message = OutboxMessage.from_record(record)
                await self._publisher.publish(subject=self._subject, payload=message.payload)
                await uow.outbox.mark_sent(message.id)
                published += 1
            if published:
                await uow.commit()
            return published
