"""Порты: протоколы UoW и репозиториев."""

from dataclasses import dataclass
from types import TracebackType
from typing import Protocol

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


class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> None: ...

    async def try_finalize(self, payment: Payment) -> bool:
        """Атомарно фиксирует финальный статус, только если он ещё pending.

        Возвращает False, если платёж уже обработан другим consumer'ом —
        в этом случае побочные эффекты (webhook) повторять нельзя.
        """
        ...

    async def get(self, payment_id: PaymentId) -> Payment | None: ...

    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...


class OutboxRepository(Protocol):
    async def add(self, *, payload: dict, aggregate_id: str) -> OutboxRecord: ...

    async def fetch_unsent(self, batch: int = 100) -> list[OutboxRecord]: ...

    async def mark_sent(self, record_id: object) -> None: ...


class AbstractUnitOfWork(Protocol):
    """Единица работы: коммит атомарен для payments + outbox."""

    payments: PaymentRepository
    outbox: OutboxRepository

    async def __aenter__(self) -> "AbstractUnitOfWork": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...
