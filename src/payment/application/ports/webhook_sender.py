"""Порт: отправка webhook-уведомлений."""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class WebhookSender(Protocol):
    async def send(self, *, url: str, payload: dict[str, Any]) -> None:
        """Подпись (HMAC) считает и ставит сам адаптер — use case её не знает."""
        ...
