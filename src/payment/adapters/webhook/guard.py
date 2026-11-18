"""Защита от SSRF: webhook_url обязан вести на публичный http(s)-адрес."""

import ipaddress
import socket
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
    """Бросает UnsafeWebhookUrl, если адрес не http(s) или указывает внутрь сети."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise UnsafeWebhookUrl(f"not a public http(s) url: {url!r}")

    host = parts.hostname
    # числовой IP проверяем напрямую, имя — после DNS-резолва
    try:
        addresses = [ipaddress.ip_address(host)]
    except ValueError:
        try:
            infos = socket.getaddrinfo(host, parts.port or 0, proto=socket.IPPROTO_TCP)
        except OSError as exc:
            raise UnsafeWebhookUrl(f"cannot resolve host: {host!r}") from exc
        addresses = [ipaddress.ip_address(info[4][0]) for info in infos]

    if not addresses:
        raise UnsafeWebhookUrl(f"cannot resolve host: {host!r}")
    for address in addresses:
        if not _is_public(address):
            raise UnsafeWebhookUrl(f"private address {address} for host {host!r}")
