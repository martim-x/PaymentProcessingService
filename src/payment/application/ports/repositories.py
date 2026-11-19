"""Порты: протоколы UoW и репозиториев."""

from dataclasses import dataclass
from datetime import datetime
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
    failed: bool = False


class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> None: ...

    async def try_claim(self, payment_id: PaymentId, ttl_seconds: float) -> datetime | None:
        """Атомарный клейм обработки: pending -> locked_until = now() + ttl.

        Возвращает токен клейма (UTC-время), либо None, если платёж уже
        финализирован или заклеймлен другим consumer'ом. Клейм истекает,
        поэтому упавший consumer не «замораживает» платёж навсегда.
        """
        ...

    async def release_claim(self, payment_id: PaymentId, claim_token: datetime) -> None:
        """Снимает клейм, если он всё ещё принадлежит claim_token."""
        ...

    async def try_finalize(self, payment: Payment, claim_token: datetime) -> bool:
        """Атомарно фиксирует финальный статус под клеймом claim_token.

        Возвращает False, если платёж уже обработан/клейм устарел —
        в этом случае побочные эффекты (webhook) повторять нельзя.
        """
        ...

    async def get(self, payment_id: PaymentId) -> Payment | None: ...

    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...


class OutboxRepository(Protocol):
    async def add(self, *, payload: dict, aggregate_id: str) -> OutboxRecord: ...

    async def fetch_unsent(self, batch: int = 100) -> list[OutboxRecord]: ...

    async def mark_sent(self, record_id: object) -> None: ...

    async def mark_attempt_failed(self, record_id: object, max_attempts: int) -> bool:
        """Инкремент attempts; True — запись окончательно помечена failed."""
        ...


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
