"""Доменные исключения."""

from typing import Any


class DomainError(Exception):
    """Базовое исключение доменного слоя."""


class InvalidMoney(DomainError):
    def __init__(self, amount: Any, reason: str) -> None:
        self.amount = amount
        self.reason = reason
        super().__init__(f"Invalid money: {reason} (amount={amount!r})")


class InvalidStatusTransition(DomainError):
    def __init__(self, current: str, target: str) -> None:
        self.current = current
        self.target = target
        super().__init__(f"Cannot transition payment from {current!r} to {target!r}")


class InvalidWebhookUrl(DomainError):
    def __init__(self, url: str) -> None:
        self.url = url
        super().__init__(f"Invalid webhook url: {url!r}")
