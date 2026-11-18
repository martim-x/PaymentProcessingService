"""Отправка webhook через httpx (с таймаутом)."""

import httpx

from payment.application.ports.webhook_sender import WebhookSender


class HttpxWebhookSender(WebhookSender):
    """POST JSON на webhook_url. Signature — заголовок X-Signature (HMAC)."""

    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self._timeout = timeout_seconds

    async def send(self, *, url: str, payload: dict, signature: str = "") -> None:
        headers = {"Content-Type": "application/json"}
        if signature:
            headers["X-Signature"] = signature
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(url, json=payload, headers=headers)
            if response.status_code >= 300:
                raise RuntimeError(f"webhook target returned {response.status_code}: {url}")
