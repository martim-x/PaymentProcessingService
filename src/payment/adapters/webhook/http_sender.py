"""Отправка webhook через httpx (таймаут, HMAC-подпись, защита от SSRF)."""

from typing import Any

import httpx

from payment.adapters.webhook.guard import WebhookUrlGuard
from payment.adapters.webhook.signer import WebhookSigner
from payment.application.ports.webhook_sender import WebhookSender


class HttpxWebhookSender(WebhookSender):
    """Сериализует, подписывает и отправляет ОДНИ И ТЕ ЖЕ байты.

    Подпись считается здесь же (а не в composition root и не в сервисе сценария):
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

    async def send(
        self,
        *,
        url: str,
        payload: dict[str, Any],
    ) -> None:
        if not self._allow_private_hosts:
            await WebhookUrlGuard.validate_async(url)

        body = WebhookSigner.serialize(payload)
        signature, timestamp = WebhookSigner.sign_body(
            self._secret,
            body,
        )
        headers = {
            "Content-Type": "application/json",
            "X-Signature": signature,
            "X-Timestamp": str(timestamp),
        }

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                url,
                content=body,
                headers=headers,
            )
            if response.status_code >= 300:
                raise RuntimeError(
                    f"webhook target returned {response.status_code}: {url}"
                )
