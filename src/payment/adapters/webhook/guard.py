"""Защита от SSRF: webhook_url обязан вести на публичный http(s)-адрес."""

import asyncio
import ipaddress
import socket
from typing import Any
from urllib.parse import urlsplit


class UnsafeWebhookUrl(ValueError):
    """Адрес не прошёл проверку на SSRF."""

    def __init__(
        self,
        *,
        message: str,
    ) -> None:
        super().__init__(message)


class WebhookUrlGuard:
    """Проверка webhook-адреса: только публичный http(s), без захода внутрь сети.

    Оговорка: проверка идёт отдельным DNS-запросом, поэтому теоретически
    возможен DNS-rebinding между проверкой и коннектом httpx. Для полной
    защиты нужен кастомный transport, подключающийся по проверенному IP;
    здесь закрыт основной класс атак (литеральные приватные адреса и
    имена, резолвящиеся внутрь сети).
    """

    @staticmethod
    def _is_public(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        # is_global отвергает CGNAT; multicast требуется исключить отдельно.
        return ip.is_global and not ip.is_multicast

    @staticmethod
    def _resolve(
        host: str,
        port: int,
    ) -> list[tuple[Any, ...]]:
        """Синхронный резолв через socket (вызывать через asyncio.to_thread)."""
        return socket.getaddrinfo(
            host,
            port,
            proto=socket.IPPROTO_TCP,
        )

    @staticmethod
    def validate(url: str) -> None:
        """Бросает UnsafeWebhookUrl, если адрес не http(s) или указывает внутрь сети."""
        parts = urlsplit(url)
        if (
            parts.scheme
            not in (
                "http",
                "https",
            )
            or not parts.hostname
        ):
            raise UnsafeWebhookUrl(message=f"not a public http(s) url: {url!r}")

        host = parts.hostname
        # числовой IP проверяем напрямую, имя — после DNS-резолва
        try:
            addresses = [ipaddress.ip_address(host)]
        except ValueError:
            try:
                infos = WebhookUrlGuard._resolve(
                    host,
                    parts.port or 0,
                )
            except OSError as exc:
                raise UnsafeWebhookUrl(
                    message=f"cannot resolve host: {host!r}"
                ) from exc
            addresses = [ipaddress.ip_address(info[4][0]) for info in infos]

        if not addresses:
            raise UnsafeWebhookUrl(message=f"cannot resolve host: {host!r}")
        for address in addresses:
            if not WebhookUrlGuard._is_public(address):
                raise UnsafeWebhookUrl(
                    message=f"private address {address} for host {host!r}"
                )

    @staticmethod
    async def validate_async(url: str) -> None:
        """Async-обёртка: DNS-резолв уходит в thread pool, event loop не блокируется."""
        await asyncio.to_thread(
            WebhookUrlGuard.validate,
            url,
        )
