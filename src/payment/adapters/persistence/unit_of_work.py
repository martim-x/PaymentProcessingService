"""Unit of Work на SQLAlchemy: payments + outbox + webhook_deliveries в одной транзакции."""

from types import TracebackType
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from payment.adapters.persistence.repositories.payment import (
    SqlAlchemyPaymentRepository,
)
from payment.adapters.persistence.repositories.webhook import (
    SqlAlchemyWebhookDeliveryRepository,
)
from payment.application.ports.repositories import (
    OutboxRepository,
    PaymentRepository,
    WebhookDeliveryRepository,
)
from payment.application.ports.unit_of_work import AbstractUnitOfWork


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    """Сессия создаётся на входе в контекст; commit фиксирует все таблицы атомарно."""

    def __init__(
        self,
        *,
        sessionmaker: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessionmaker = sessionmaker
        self._session: Optional[AsyncSession] = None
        self._in_use = False
        self._payments: PaymentRepository
        self._outbox: OutboxRepository
        self._webhook_deliveries: WebhookDeliveryRepository

    @property
    def session(self) -> Optional[AsyncSession]:
        return self._session

    async def __aenter__(self) -> "SqlAlchemyUnitOfWork":
        if self._in_use:
            # UoW хранит сессию: параллельное использование одного экземпляра
            # смешало бы транзакции — это ошибка времени жизни зависимости
            raise RuntimeError(
                "UnitOfWork is already in use and is not concurrency-safe"
            )
        self._in_use = True
        session = self._sessionmaker()
        self._session = session
        self._payments = SqlAlchemyPaymentRepository(session=session)
        self._outbox = SqlAlchemyOutboxRepository(session=session)
        self._webhook_deliveries = SqlAlchemyWebhookDeliveryRepository(session=session)
        return self

    @property
    def payments(self) -> PaymentRepository:
        return self._payments

    @payments.setter
    def payments(
        self,
        repository: PaymentRepository,
    ) -> None:
        self._payments = repository

    @property
    def outbox(self) -> OutboxRepository:
        return self._outbox

    @outbox.setter
    def outbox(
        self,
        repository: OutboxRepository,
    ) -> None:
        self._outbox = repository

    @property
    def webhook_deliveries(self) -> WebhookDeliveryRepository:
        return self._webhook_deliveries

    @webhook_deliveries.setter
    def webhook_deliveries(
        self,
        repository: WebhookDeliveryRepository,
    ) -> None:
        self._webhook_deliveries = repository

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        try:
            if exc is not None:
                await self.rollback()
        finally:
            assert self._session is not None
            await self._session.close()
            self._session = None
            self._in_use = False

    async def commit(self) -> None:
        assert self._session is not None
        await self._session.commit()

    async def rollback(self) -> None:
        assert self._session is not None
        await self._session.rollback()
