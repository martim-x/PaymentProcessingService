"""Отправка webhook через httpx (таймаут, HMAC-подпись, защита от SSRF)."""

import httpx

from payment.adapters.webhook.guard import validate_public_http_url_async
from payment.adapters.webhook.signer import serialize, sign_body
from payment.application.ports.webhook_sender import WebhookSender


class HttpxWebhookSender(WebhookSender):
    """Сериализует, подписывает и отправляет ОДНИ И ТЕ ЖЕ байты.

    Подпись считается здесь же (а не в composition root и не в use case):
    сериализация и HMAC обязаны быть в одном месте, иначе они разъедутся.
    """

    def __init__(
        self,
        *,
        secret: str,
        timeout_seconds: float = 5.0,
        allow_private_hosts: bool = False,
    ) -> None:
        self._secret = secret
        self._timeout = timeout_seconds
        self._allow_private_hosts = allow_private_hosts

    async def send(self, *, url: str, payload: dict) -> None:
        if not self._allow_private_hosts:
            await validate_public_http_url_async(url)

        body = serialize(payload)
        signature, timestamp = sign_body(self._secret, body)
        headers = {
            "Content-Type": "application/json",
            "X-Signature": signature,
            "X-Timestamp": str(timestamp),
        }

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(url, content=body, headers=headers)
            if response.status_code >= 300:
                raise RuntimeError(f"webhook target returned {response.status_code}: {url}")
