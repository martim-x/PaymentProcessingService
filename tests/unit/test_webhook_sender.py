"""HttpxWebhookSender: отправляет ровно те байты, от которых посчитана подпись."""

import httpx

from payment.adapters.webhook.http_sender import HttpxWebhookSender
from payment.adapters.webhook.signer import serialize, sign_body, verify_body

SECRET = "unit-secret"
PAYLOAD = {"id": "p1", "status": "succeeded", "amount": "10.0000", "description": "Заказ"}


class TestHttpxWebhookSender:
    async def test_body_matches_signature_bytes(self, monkeypatch) -> None:
        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = request.content
            captured["headers"] = dict(request.headers)
            return httpx.Response(200)

        transport = httpx.MockTransport(handler)
        real_client = httpx.AsyncClient

        def client_factory(*args, **kwargs):
            kwargs["transport"] = transport
            return real_client(*args, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client_factory)

        body = serialize(PAYLOAD)
        _, timestamp = sign_body(SECRET, body)
        sender = HttpxWebhookSender(secret=SECRET, timeout_seconds=1.0, allow_private_hosts=True)

        await sender.send(url="http://example.com/hook", payload=PAYLOAD)

        assert captured["body"] == body
        assert verify_body(SECRET, captured["body"], captured["headers"]["x-signature"])
        assert captured["headers"]["x-timestamp"] == str(timestamp)

    async def test_non_2xx_raises(self, monkeypatch) -> None:
        transport = httpx.MockTransport(lambda request: httpx.Response(503))
        real_client = httpx.AsyncClient

        def client_factory(*args, **kwargs):
            kwargs["transport"] = transport
            return real_client(*args, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client_factory)
        sender = HttpxWebhookSender(secret=SECRET, timeout_seconds=1.0, allow_private_hosts=True)

        try:
            await sender.send(url="http://example.com/hook", payload=PAYLOAD)
        except RuntimeError as exc:
            assert "503" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected RuntimeError for non-2xx response")
