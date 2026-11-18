"""Порт: внешний платёжный шлюз."""

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class GatewayResult:
    succeeded: bool
    reason: Optional[str] = None


@runtime_checkable
class PaymentGateway(Protocol):
    async def process(self, *, payment_id: str, amount: object, currency: str) -> GatewayResult: ...
