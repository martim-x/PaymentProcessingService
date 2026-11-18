"""Репозиторий платежей на SQLAlchemy (ручной маппинг домена)."""

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment.adapters.persistence.orm import payments_table
from payment.domain.model.enums import Currency, PaymentStatus
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import IdempotencyKey, Money, PaymentId, WebhookUrl


def _to_domain(row) -> Payment:
    return Payment(
        id=PaymentId(value=row.id),
        amount=Money(amount=row.amount, currency=Currency(row.currency)),
        description=row.description,
        metadata=dict(row._mapping["metadata"]),
        idempotency_key=IdempotencyKey(row.idempotency_key),
        webhook_url=WebhookUrl(row.webhook_url),
        status=PaymentStatus(row.status),
        fingerprint=row.fingerprint,
        failure_reason=row.failure_reason,
        created_at=row.created_at,
        processed_at=row.processed_at,
    )


def _to_row_values(payment: Payment) -> dict:
    return {
        "id": payment.id.value,
        "amount": payment.amount.amount,
        "currency": payment.amount.currency.value,
        "description": payment.description,
        "metadata": dict(payment.metadata),
        "status": payment.status.value,
        "fingerprint": payment.fingerprint,
        "idempotency_key": payment.idempotency_key.value,
        "webhook_url": payment.webhook_url.value,
        "failure_reason": payment.failure_reason,
        "created_at": payment.created_at,
        "processed_at": payment.processed_at,
    }


class SqlAlchemyPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, payment: Payment) -> None:
        await self._session.execute(insert(payments_table).values(**_to_row_values(payment)))

    async def try_finalize(self, payment: Payment) -> bool:
        """UPDATE ... WHERE id = :id AND status = 'pending' — защита от двойной обработки."""
        result = await self._session.execute(
            update(payments_table)
            .where(
                payments_table.c.id == payment.id.value,
                payments_table.c.status == PaymentStatus.PENDING.value,
            )
            .values(
                status=payment.status.value,
                failure_reason=payment.failure_reason,
                processed_at=payment.processed_at,
            )
            .returning(payments_table.c.id)
        )
        return result.first() is not None

    async def get(self, payment_id: PaymentId) -> Payment | None:
        result = await self._session.execute(
            select(payments_table).where(payments_table.c.id == payment_id.value)
        )
        row = result.first()
        return _to_domain(row) if row is not None else None

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        result = await self._session.execute(
            select(payments_table).where(payments_table.c.idempotency_key == key)
        )
        row = result.first()
        return _to_domain(row) if row is not None else None
