"""FastStream-реализация MessagePublisher.

Различает два класса ошибок публикации ЯВНЫМ списком типов (без эвристик
по именам классов — иначе «ядовитая» запись с подходящим именем исключения
блокировала бы батч на каждом проходе):

- недоступность транспорта (соединение/канал/неподключённый брокер)
  → `PublisherUnavailable`: лечится ожиданием, попытки записи outbox не тратятся;
- отказ конкретной записи (`DeliveryError`, `PublishError`, сериализация)
  → пробрасывается как есть, запись получает +1 к attempts.

Каждая публикация:
- `persist=True` — сообщение переживает рестарт RabbitMQ;
- `mandatory=True` + `on_return_raises=True` на канале — публикация в
  несуществующую очередь не «успешна молча», а падает `DeliveryError`
  (запись outbox не помечается sent);
- publisher confirms включены: без подтверждения от брокера публикация
  считается недоступностью транспорта и повторяется следующим проходом.
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
from pamqp.commands import Basic

from payment.adapters.messaging.topology import MAIN_QUEUE
from payment.application.ports.message_publisher import (
    MessagePublisher,
    PublisherUnavailable,
    PublishRejected,
)

# только транспортные ошибки: брокер/канал недоступны, ожидание лечит
_UNAVAILABLE_TYPES: tuple[type[BaseException], ...] = (
    AMQPConnectionError,
    ConnectionClosed,
    ChannelClosed,
    ChannelInvalidStateError,
    IncorrectState,  # FastStream: публикация в неподключённый брокер
    OSError,  # ConnectionError и TimeoutError — его подклассы, отдельно не нужны
)


def _is_unavailable(exc: BaseException) -> bool:
    if isinstance(
        exc,
        PublisherUnavailable,
    ):
        return True
    return isinstance(
        exc,
        _UNAVAILABLE_TYPES,
    )


class FastStreamPublisher(MessagePublisher):
    """Публикует сообщения в RabbitMQ и проверяет подтверждение брокера."""

    def __init__(
        self,
        *,
        broker: RabbitBroker,
    ) -> None:
        self._broker = broker

    async def publish(
        self,
        *,
        subject: str,
        payload: dict[str, Any],
        message_id: str | None = None,
    ) -> None:
        try:
            # subject — имя очереди (маршрутизация через default exchange)
            confirmation = await self._broker.publish(
                payload,
                queue=subject or MAIN_QUEUE,
                persist=True,
                mandatory=True,
                message_id=message_id,
            )
        except Exception as exc:
            if _is_unavailable(exc):
                raise PublisherUnavailable(
                    message=f"{type(exc).__name__}: {exc}"
                ) from exc
            raise
        ensure_confirmed(
            confirmation,
            subject,
        )


def ensure_confirmed(
    confirmation: object,
    subject: str,
) -> None:
    """Publisher confirms: None и basic.nack — НЕ успешная публикация.

    - None: подтверждения выключены / соединение закрылось до ответа — это
      недоступность транспорта, запись outbox повторяется без штрафа;
    - `basic.nack` (или False в старых клиентах): брокер отказал в приёме —
      ошибка конкретной записи, outbox считает попытку;
    - `basic.return` при `on_return_raises=True` превращается в DeliveryError
      ещё внутри publish, сюда не доходит.
    """
    if confirmation is None:
        raise PublisherUnavailable(message="broker did not confirm the publish")
    if (
        isinstance(
            confirmation,
            Basic.Nack,
        )
        or confirmation is False
    ):
        raise PublishRejected(
            message=f"broker rejected the publish with basic.nack (queue={subject})"
        )
