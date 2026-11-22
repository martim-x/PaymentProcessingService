"""Эмуляция внешнего платёжного шлюза: 2-5 сек, 90% успех / 10% ошибка."""

import asyncio
import random

from payment.application.ports.payment_gateway import GatewayResult, PaymentGateway


class FakePaymentGateway(PaymentGateway):
    """Эмулирует внешний шлюз с настраиваемыми задержкой и вероятностью успеха."""

    def __init__(
        self,
        *,
        min_seconds: float = 2.0,
        max_seconds: float = 5.0,
        success_rate: float = 0.9,
        rng: random.Random | None = None,
    ) -> None:
        self._min = min_seconds
        self._max = max_seconds
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def process(
        self,
        *,
        payment_id: str,
        amount: object,
        currency: str,
        idempotency_key: str = "",
    ) -> GatewayResult:
        delay = self._rng.uniform(
            self._min,
            self._max,
        )
        await asyncio.sleep(delay)
        if self._rng.random() < self._success_rate:
            return GatewayResult(succeeded=True)
        return GatewayResult(
            succeeded=False,
            reason="random gateway failure",
        )
