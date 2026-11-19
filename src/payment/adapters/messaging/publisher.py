"""FastStream-реализация MessagePublisher.

Различает два класса ошибок публикации ЯВНЫМ списком типов (без эвристик
по именам классов — иначе «ядовитая» запись с подходящим именем исключения
блокировала бы батч на каждом проходе):

- недоступность транспорта (соединение/канал/неподключённый брокер)
  → `PublisherUnavailable`: лечится ожиданием, попытки записи outbox не тратятся;
- отказ конкретной записи (`DeliveryError`, `PublishError`, сериализация)
  → пробрасывается как есть, запись получает +1 к attempts.
"""

from typing import Any

from aio_pika.exceptions import (
    AMQPConnectionError,
    ChannelClosed,
    ChannelInvalidStateError,
    ConnectionClosed,
)
from faststream.exceptions import IncorrectState
from faststream.rabbit import RabbitBroker

from payment.adapters.messaging.topology import MAIN_QUEUE
from payment.application.ports.message_publisher import MessagePublisher, PublisherUnavailable

# только транспортные ошибки: брокер/канал недоступны, ожидание лечит
_UNAVAILABLE_TYPES: tuple[type[BaseException], ...] = (
    AMQPConnectionError,
    ConnectionClosed,
    ChannelClosed,
    ChannelInvalidStateError,
    IncorrectState,  # FastStream: публикация в неподключённый брокер
    ConnectionError,
    OSError,
    TimeoutError,
)


def _is_unavailable(exc: BaseException) -> bool:
    if isinstance(exc, PublisherUnavailable):
        return True
    return isinstance(exc, _UNAVAILABLE_TYPES)


class FastStreamPublisher(MessagePublisher):
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, *, subject: str, payload: dict[str, Any]) -> None:
        try:
            # subject — имя очереди (маршрутизация через default exchange)
            await self._broker.publish(payload, queue=subject or MAIN_QUEUE)
        except Exception as exc:
            if _is_unavailable(exc):
                raise PublisherUnavailable(f"{type(exc).__name__}: {exc}") from exc
            raise
