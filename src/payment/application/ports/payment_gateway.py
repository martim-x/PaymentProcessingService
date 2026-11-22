"""Порт: внешний платёжный шлюз."""

from dataclasses import dataclass
from typing import Protocol


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class GatewayResult:
    """Итог вызова шлюза: отказ — succeeded=False с причиной, а не исключение."""

    succeeded: bool
    reason: str | None = None


class PaymentGateway(Protocol):
    """Контракт обработки платежа внешним шлюзом."""

    async def process(
        self,
        *,
        payment_id: str,
        amount: object,
        currency: str,
        idempotency_key: str = "",
    ) -> GatewayResult:
        """Проводит списание; бизнес-отказ возвращается как GatewayResult.

        Исключение означает сбой транспорта или таймаут — это повод для retry
        сообщения. idempotency_key защищает от повторного списания при повторе.
        """
        ...
