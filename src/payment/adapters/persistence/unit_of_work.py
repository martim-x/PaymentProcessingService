"""Unit of Work на SQLAlchemy: payments + outbox в одной транзакции."""

from types import TracebackType
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from payment.adapters.persistence.repositories.payment import SqlAlchemyPaymentRepository
from payment.application.ports.repositories import (
    AbstractUnitOfWork,
    OutboxRepository,
    PaymentRepository,
)


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    """Сессия создаётся на входе в контекст; commit фиксирует payments+outbox атомарно."""

    def __init__(self, sessionmaker: async_sessionmaker) -> None:
        self._sessionmaker = sessionmaker
        self._session: Optional[AsyncSession] = None

    @property
    def session(self) -> Optional[AsyncSession]:
        return self._session

    async def __aenter__(self) -> "SqlAlchemyUnitOfWork":
        self._session = self._sessionmaker()
        session = self._session
        assert session is not None
        self.payments: PaymentRepository = SqlAlchemyPaymentRepository(session)
        self.outbox: OutboxRepository = SqlAlchemyOutboxRepository(session)
        return self

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

    async def commit(self) -> None:
        assert self._session is not None
        await self._session.commit()

    async def rollback(self) -> None:
        assert self._session is not None
        await self._session.rollback()
