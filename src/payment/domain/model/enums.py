"""Перечисления домена."""

from enum import Enum


class Currency(
    str,
    Enum,
):
    """Поддерживаемые валюты платежей."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(
    str,
    Enum,
):
    """Состояния жизненного цикла платежа."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
