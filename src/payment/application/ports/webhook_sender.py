"""Порт: отправка webhook-уведомлений."""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class WebhookSender(Protocol):
    async def send(self, *, url: str, payload: dict[str, Any], signature: str = "") -> None: ...
