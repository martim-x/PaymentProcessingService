"""Порты: протоколы UoW и репозиториев."""

from dataclasses import dataclass
from types import TracebackType
from typing import Optional, Protocol, runtime_checkable

from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId


@dataclass(slots=True)
class OutboxRecord:
    """Строка outbox (доменно-нейтральная)."""

    id: object
    aggregate_id: str
    payload: dict
    sent: bool = False
    attempts: int = 0


@runtime_checkable
class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> None: ...

    async def save(self, payment: Payment) -> None: ...

    async def get(self, payment_id: PaymentId) -> Optional[Payment]: ...

    async def get_by_idempotency_key(self, key: str) -> Optional[Payment]: ...


@runtime_checkable
class OutboxRepository(Protocol):
    async def add(self, *, payload: dict, aggregate_id: str) -> OutboxRecord: ...

    async def fetch_unsent(self, batch: int = 100) -> list[OutboxRecord]: ...

    async def mark_sent(self, record_id) -> None: ...


@runtime_checkable
class AbstractUnitOfWork(Protocol):
    """Единица работы: коммит атомарен для payments + outbox."""

    payments: PaymentRepository
    outbox: OutboxRepository

    async def __aenter__(self) -> "AbstractUnitOfWork": ...

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...
