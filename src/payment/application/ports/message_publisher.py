"""Порт: публикация сообщений в брокер."""

from typing import Any, Protocol


class PublisherUnavailable(Exception):
    """Брокер недоступен (соединение/канал): это НЕ ошибка конкретной записи.

    Батч outbox не должен штрафовать записи за недоступность брокера —
    иначе рестарт RabbitMQ на пару секунд «сожжёт» весь лимит попыток.
    """


class PublishRejected(Exception):
    """Брокер отверг конкретную публикацию (basic.nack / unroutable).

    Это НЕ недоступность транспорта: запись outbox получает +1 к attempts,
    иначе «ядовитая» запись ретраилась бы бесконечно.
    """


class MessagePublisher(Protocol):
    async def publish(
        self,
        *,
        subject: str,
        payload: dict[str, Any],
        message_id: str | None = None,
    ) -> None:
        """Публикация с подтверждением брокера (publisher confirms, persistent).

        `message_id` — стабильный id события: получатель может дедуплицировать
        повторную публикацию того же события.
        """
        ...
