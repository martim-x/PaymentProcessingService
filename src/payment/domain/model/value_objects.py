"""Значимые объекты (Value Objects) домена: неизменяемые, валидируются при создании.

Денежные и URL-инварианты нарушаются доменными исключениями (InvalidMoney,
InvalidWebhookUrl) — HTTP-слой отдаёт по ним 422. IdempotencyKey остаётся
на уровне ValueError: это ошибка входа, а не бизнес-правило.
"""

import ipaddress
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
    """Сумма с валютой, приведённая к точности БД.

    Квантование до 4 знаков (ROUND_HALF_UP) идёт в конструкторе, поэтому
    сохранение и сравнение работают уже с нормализованным значением.
    Нулевые, отрицательные и не-конечные суммы (NaN/Infinity) отсекаются.
    """

    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise InvalidMoney(self.amount, "amount must be Decimal")
        if not self.amount.is_finite() or self.amount <= 0:
            raise InvalidMoney(self.amount, "amount must be positive")
        # нормализация точности до 4 знаков (Numeric(19,4) в БД)
        try:
            quantized = self.amount.quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)
        except InvalidOperation as exc:
            raise InvalidMoney(self.amount, "amount exceeds the supported precision") from exc
        if quantized <= 0:
            raise InvalidMoney(self.amount, "amount rounds to zero at the supported precision")
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
        """ValueError, если строка не UUID: API трактует это как 404, а не 500."""
        return PaymentId(value=uuid.UUID(raw))

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    """Ключ клиента для защиты от дублей: непустой, до 255 символов.

    Значение не нормализуется: ключ сравнивается ровно так, как пришёл,
    поэтому регистр и пробелы по краям значимы.
    """

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value.strip():
            raise ValueError("IdempotencyKey must be a non-empty string")
        if len(self.value) > 255:
            raise ValueError("IdempotencyKey must be at most 255 characters")
        object.__setattr__(self, "value", self.value)


@dataclass(frozen=True, slots=True)
class WebhookUrl:
    """Адрес уведомления: только http(s) с непустым хостом.

    Литеральный приватный или loopback-IP отклоняется сразу, без DNS;
    имена, резолвящиеся внутрь сети, проверяет адаптер перед отправкой.
    """

    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise InvalidWebhookUrl(self.value)
        parsed = urlsplit(self.value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise InvalidWebhookUrl(self.value)
        # литеральный приватный/loopback IP отклоняем сразу (без DNS) — 422 на API
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            return
        if not _ip_is_public(address):
            raise InvalidWebhookUrl(self.value)


def _ip_is_public(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    # Multicast может иметь is_global=True, поэтому проверяется отдельно.
    return address.is_global and not address.is_multicast


def to_decimal(raw: Any) -> Decimal:
    """Приводит ввод API (str/int/float) к Decimal; мусор — InvalidMoney.

    Через str(): Decimal(float) сохранил бы двоичное представление
    (0.1 -> 0.1000...055), а не десятичное 0.1.
    """
    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise InvalidMoney(raw, "not a valid decimal") from exc
