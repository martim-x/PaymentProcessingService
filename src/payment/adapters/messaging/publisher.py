"""FastStream-реализация MessagePublisher.

Различает два класса ошибок публикации:
- недоступность транспорта (соединение/канал) → `PublisherUnavailable`:
  лечится ожиданием, попытки записи outbox не тратятся;
- отказ конкретной записи (например, сериализация/роутинг) → пробрасывается
  как есть, и запись получает +1 к attempts.
"""

from typing import Any

from aio_pika.exceptions import AMQPChannelError, AMQPConnectionError, ConnectionClosed
from faststream.rabbit import RabbitBroker

from payment.adapters.messaging.topology import MAIN_QUEUE
from payment.application.ports.message_publisher import MessagePublisher, PublisherUnavailable

# явные классы известных библиотек; ниже — name-based fallback для обёрток faststream
_UNAVAILABLE_TYPES: tuple[type[BaseException], ...] = (
    AMQPConnectionError,
    AMQPChannelError,
    ConnectionClosed,
    ConnectionError,
    OSError,
    TimeoutError,
)
_UNAVAILABLE_NAME_MARKERS = ("Connection", "ChannelInvalidState", "ChannelClosed", "Timeout")
# отказ доставки конкретного сообщения — это не недоступность брокера
_PER_RECORD_MARKERS = ("DeliveryError",)


def _is_unavailable(exc: BaseException) -> bool:
    names = " ".join(cls.__name__ for cls in type(exc).__mro__)
    if any(marker in names for marker in _PER_RECORD_MARKERS):
        return False
    if isinstance(exc, _UNAVAILABLE_TYPES):
        return True
    if isinstance(exc, PublisherUnavailable):
        return True
    return any(marker in names for marker in _UNAVAILABLE_NAME_MARKERS)


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
