"""Отправка webhook через httpx (таймаут, HMAC-подпись, защита от SSRF)."""

import httpx

from payment.adapters.webhook.guard import validate_public_http_url_async
from payment.adapters.webhook.signer import serialize
from payment.application.ports.webhook_sender import WebhookSender


class HttpxWebhookSender(WebhookSender):
    """POST тех же байтов, от которых посчитана подпись X-Signature."""

    def __init__(self, *, timeout_seconds: float = 5.0, allow_private_hosts: bool = False) -> None:
        self._timeout = timeout_seconds
        self._allow_private_hosts = allow_private_hosts

    async def send(self, *, url: str, payload: dict, signature: str = "") -> None:
        if not self._allow_private_hosts:
            await validate_public_http_url_async(url)

        body = serialize(payload)
        headers = {"Content-Type": "application/json"}
        if signature:
            headers["X-Signature"] = signature
            ts_part = dict(p.split("=", 1) for p in signature.split(",") if "=" in p).get("t")
            if ts_part:
                headers["X-Timestamp"] = ts_part

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(url, content=body, headers=headers)
            if response.status_code >= 300:
                raise RuntimeError(f"webhook target returned {response.status_code}: {url}")
