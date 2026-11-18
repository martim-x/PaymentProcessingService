"""Защита от SSRF: webhook_url обязан вести на публичный http(s)-адрес."""

import asyncio
import ipaddress
from urllib.parse import urlsplit


class UnsafeWebhookUrl(ValueError):
    """Адрес не прошёл проверку на SSRF."""


def _is_public(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def validate_public_http_url(url: str) -> None:
    """Бросает UnsafeWebhookUrl, если адрес не http(s) или указывает внутрь сети.

    Оговорка: проверка идёт отдельным DNS-запросом, поэтому теоретически
    возможен DNS-rebinding между проверкой и коннектом httpx. Для полной
    защиты нужен кастомный transport, подключающийся по проверенному IP;
    здесь закрыт основной класс атак (литеральные приватные адреса и
    имена, резолвящиеся внутрь сети).
    """
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise UnsafeWebhookUrl(f"not a public http(s) url: {url!r}")

    host = parts.hostname
    # числовой IP проверяем напрямую, имя — после DNS-резолва
    try:
        addresses = [ipaddress.ip_address(host)]
    except ValueError:
        try:
            infos = socket_getaddrinfo(host, parts.port or 0)
        except OSError as exc:
            raise UnsafeWebhookUrl(f"cannot resolve host: {host!r}") from exc
        addresses = [ipaddress.ip_address(info[4][0]) for info in infos]

    if not addresses:
        raise UnsafeWebhookUrl(f"cannot resolve host: {host!r}")
    for address in addresses:
        if not _is_public(address):
            raise UnsafeWebhookUrl(f"private address {address} for host {host!r}")


def socket_getaddrinfo(host: str, port: int):
    """Синхронный резолв через socket (вызывать через asyncio.to_thread)."""
    import socket

    return socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)


async def validate_public_http_url_async(url: str) -> None:
    """Async-обёртка: DNS-резолв уходит в thread pool, event loop не блокируется."""
    await asyncio.to_thread(validate_public_http_url, url)
