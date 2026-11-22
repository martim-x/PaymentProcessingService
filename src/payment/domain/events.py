"""Доменные события агрегата Payment."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from payment.domain.model.enums import Currency


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class PaymentCreated:
    """Факт создания платежа с данными для дальнейшего уведомления."""

    payment_id: UUID
    amount: Decimal
    currency: Currency
    idempotency_key: str
    webhook_url: str
    metadata: dict[str, Any]


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class PaymentSucceeded:
    """Факт успешной обработки платежа."""

    payment_id: UUID
    processed_at: datetime


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class PaymentFailed:
    """Факт неуспешной обработки платежа с причиной отказа."""

    payment_id: UUID
    reason: str
    processed_at: datetime
