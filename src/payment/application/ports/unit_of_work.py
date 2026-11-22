"""Порт единицы работы."""

from types import TracebackType
from typing import Protocol, Self

from payment.application.ports.repositories import (
    OutboxRepository,
    PaymentRepository,
    WebhookDeliveryRepository,
)


class AbstractUnitOfWork(Protocol):
    """Единица работы: коммит атомарен для payments + outbox + webhook_deliveries."""

    @property
    def payments(self) -> PaymentRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    @property
    def webhook_deliveries(self) -> WebhookDeliveryRepository: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...


__all__ = ["AbstractUnitOfWork"]
