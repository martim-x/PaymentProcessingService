"""Значимые объекты (Value Objects) домена."""

import uuid
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit

from payment.domain.exceptions import InvalidMoney, InvalidWebhookUrl
from payment.domain.model.enums import Currency

_MONEY_QUANT = Decimal("0.0001")
# Numeric(19,4): 15 целых цифр + 4 дробных
_MONEY_MAX = Decimal("999999999999999.9999")


@dataclass(frozen=True, slots=True)
class Money:
    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise InvalidMoney(self.amount, "amount must be Decimal")
        if self.amount <= 0:
            raise InvalidMoney(self.amount, "amount must be positive")
        # нормализация точности до 4 знаков (Numeric(19,4) в БД)
        quantized = self.amount.quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)
        if quantized > _MONEY_MAX:
            raise InvalidMoney(self.amount, "amount exceeds the supported maximum")
        object.__setattr__(self, "amount", quantized)


@dataclass(frozen=True, slots=True)
class PaymentId:
    value: uuid.UUID

    @staticmethod
    def next() -> "PaymentId":
        return PaymentId(value=uuid.uuid4())

    @staticmethod
    def from_string(raw: str) -> "PaymentId":
        return PaymentId(value=uuid.UUID(raw))

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value.strip():
            raise ValueError("IdempotencyKey must be a non-empty string")
        if len(self.value) > 255:
            raise ValueError("IdempotencyKey must be at most 255 characters")
        object.__setattr__(self, "value", self.value)


@dataclass(frozen=True, slots=True)
class WebhookUrl:
    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise InvalidWebhookUrl(self.value)
        parsed = urlsplit(self.value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise InvalidWebhookUrl(self.value)


def to_decimal(raw: Any) -> Decimal:
    """Преобразует сырой ввод API (str/int/float) в Decimal."""
    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise InvalidMoney(raw, "not a valid decimal") from exc
