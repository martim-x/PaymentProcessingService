"""Значимые объекты (Value Objects) домена: неизменяемые, валидируются при создании.

Денежные и URL-инварианты нарушаются доменными исключениями (InvalidMoney,
InvalidWebhookUrl) — HTTP-слой отдаёт по ним 422. IdempotencyKey остаётся
на уровне ValueError: это ошибка входа, а не бизнес-правило.
"""

import ipaddress
import uuid
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, ClassVar, cast
from urllib.parse import urlsplit

from payment.domain.exceptions import InvalidMoney, InvalidWebhookUrl
from payment.domain.model.enums import Currency


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class Money:
    """Сумма с валютой, приведённая к точности БД.

    Квантование до 4 знаков (ROUND_HALF_UP) идёт в конструкторе, поэтому
    сохранение и сравнение работают уже с нормализованным значением.
    Нулевые, отрицательные и не-конечные суммы (NaN/Infinity) отсекаются.
    """

    _MONEY_QUANT: ClassVar[Decimal] = Decimal("0.0001")
    # Numeric(19,4): 15 целых цифр + 4 дробных
    _MONEY_MAX: ClassVar[Decimal] = Decimal("999999999999999.9999")

    amount: Decimal
    currency: Currency

    @staticmethod
    def from_raw(*, raw: Any) -> Decimal:
        """Convert input via str, preserving the established Decimal semantics."""
        try:
            return Decimal(str(raw))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise InvalidMoney(amount=raw, reason="not a valid decimal") from exc

    def __post_init__(self) -> None:
        if not isinstance(
            cast(
                object,
                self.amount,
            ),
            Decimal,
        ):
            raise InvalidMoney(
                amount=self.amount,
                reason="amount must be Decimal",
            )
        if not self.amount.is_finite() or self.amount <= 0:
            raise InvalidMoney(
                amount=self.amount,
                reason="amount must be positive",
            )
        # нормализация точности до 4 знаков (Numeric(19,4) в БД)
        try:
            quantized = self.amount.quantize(
                self._MONEY_QUANT,
                rounding=ROUND_HALF_UP,
            )
        except InvalidOperation as exc:
            raise InvalidMoney(
                amount=self.amount,
                reason="amount exceeds the supported precision",
            ) from exc
        if quantized <= 0:
            raise InvalidMoney(
                amount=self.amount,
                reason="amount rounds to zero at the supported precision",
            )
        if quantized > self._MONEY_MAX:
            raise InvalidMoney(
                amount=self.amount,
                reason="amount exceeds the supported maximum",
            )
        object.__setattr__(
            self,
            "amount",
            quantized,
        )


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class PaymentId:
    """Типизированный идентификатор платежа."""

    value: uuid.UUID

    @staticmethod
    def next() -> "PaymentId":
        return PaymentId(value=uuid.uuid4())

    @staticmethod
    def from_string(*, raw: str) -> "PaymentId":
        """ValueError, если строка не UUID: API трактует это как 404, а не 500."""
        return PaymentId(value=uuid.UUID(raw))

    def __str__(self) -> str:
        return str(self.value)


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class IdempotencyKey:
    """Ключ клиента для защиты от дублей: непустой, до 255 символов."""

    MAX_LENGTH: ClassVar[int] = 255
    value: str

    def __post_init__(self) -> None:
        if not isinstance(cast(object, self.value), str) or not self.value.strip():
            raise ValueError("IdempotencyKey must be a non-empty string")
        if len(self.value) > self.MAX_LENGTH:
            raise ValueError(
                f"IdempotencyKey must be at most {self.MAX_LENGTH} characters"
            )
        object.__setattr__(self, "value", self.value)


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class WebhookUrl:
    """Адрес уведомления: только http(s) с непустым хостом.

    Литеральный приватный или loopback-IP отклоняется сразу, без DNS;
    имена, резолвящиеся внутрь сети, проверяет адаптер перед отправкой.
    """

    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise InvalidWebhookUrl(url=self.value)
        parsed = urlsplit(self.value)
        if (
            parsed.scheme
            not in (
                "http",
                "https",
            )
            or not parsed.hostname
        ):
            raise InvalidWebhookUrl(url=self.value)
        # литеральный приватный/loopback IP отклоняем сразу (без DNS) — 422 на API
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            return
        if not self._ip_is_public(address):
            raise InvalidWebhookUrl(url=self.value)

    @staticmethod
    def _ip_is_public(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        # Multicast может иметь is_global=True, поэтому проверяется отдельно.
        return address.is_global and not address.is_multicast
