"""DTO (Command/Result объекты; не Pydantic-схемы API)."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from payment.application.ports.repositories import OutboxRecord
from payment.domain.model.enums import Currency
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import IdempotencyKey


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    idempotency_key: IdempotencyKey
    webhook_url: str
    fingerprint: str


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


@dataclass(frozen=True, slots=True)
class ProcessPaymentResult:
    payment_id: UUID
    status: str
    failure_reason: Optional[str] = None


@dataclass(frozen=True, slots=True)
class PaymentDetails:
    """Плоская проекция платежа для API."""

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: str
    failure_reason: Optional[str]
    webhook_url: str
    created_at: datetime
    processed_at: Optional[datetime]

    @staticmethod
    def from_payment(payment: Payment) -> "PaymentDetails":
        return PaymentDetails(
            id=payment.id.value,
            amount=payment.amount.amount,
            currency=payment.amount.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status.value,
            failure_reason=payment.failure_reason,
            webhook_url=payment.webhook_url.value,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """Сообщение, готовое к публикации в брокер."""

    id: UUID
    aggregate_id: str
    payload: dict[str, Any]

    @staticmethod
    def from_record(record: OutboxRecord) -> "OutboxMessage":
        return OutboxMessage(id=record.id, aggregate_id=record.aggregate_id, payload=record.payload)
