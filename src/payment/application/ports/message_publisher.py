"""Порт: публикация сообщений в брокер."""

from typing import Any, Protocol


class PublisherUnavailable(Exception):
    """Брокер недоступен (соединение/канал): это НЕ ошибка конкретной записи.

    Батч outbox не должен штрафовать записи за недоступность брокера —
    иначе рестарт RabbitMQ на пару секунд «сожжёт» весь лимит попыток.
    """


class MessagePublisher(Protocol):
    async def publish(self, *, subject: str, payload: dict[str, Any]) -> None: ...
