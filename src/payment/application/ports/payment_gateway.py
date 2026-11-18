"""Порт: внешний платёжный шлюз."""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class GatewayResult:
    succeeded: bool
    reason: str | None = None


class PaymentGateway(Protocol):
    async def process(
        self, *, payment_id: str, amount: object, currency: str, idempotency_key: str = ""
    ) -> GatewayResult:
        """idempotency_key защищает от повторного списания у реального шлюза."""
        ...
