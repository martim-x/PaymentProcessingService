"""Порт: отправка webhook-уведомлений."""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class WebhookSender(Protocol):
    async def send(self, *, url: str, payload: dict[str, Any]) -> None:
        """Отправляет тело на url; не-2xx и сетевые сбои — исключение (retry/DLQ).

        Сериализация и HMAC-подпись — обязанность адаптера: сервис передаёт
        dict и о формате подписи не знает.
        """
        ...
