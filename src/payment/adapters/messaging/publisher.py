"""FastStream-реализация MessagePublisher."""

from typing import Any

from faststream.rabbit import RabbitBroker

from payment.adapters.messaging.topology import MAIN_QUEUE
from payment.application.ports.message_publisher import MessagePublisher


class FastStreamPublisher(MessagePublisher):
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, *, subject: str, payload: dict[str, Any]) -> None:
        # subject — имя очереди (маршрутизация через default exchange)
        await self._broker.publish(payload, queue=subject or MAIN_QUEUE)
