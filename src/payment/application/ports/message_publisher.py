"""Порт: публикация сообщений в брокер."""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class MessagePublisher(Protocol):
    async def publish(self, *, subject: str, payload: dict[str, Any]) -> None: ...
