"""E2E: полный сценарий против работающего docker compose стека.

Запуск локально:  docker compose up --build -d  &&  pytest tests/e2e -q
В CI: поднимается job'ом e2e.

Требует: PAYMENT_API_URL (по умолчанию http://localhost:8000), PAYMENT_API_KEY,
PAYMENT_WEBHOOK_SECRET (должен совпадать с секретом сервиса).
"""

import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest

from payment.adapters.webhook.signer import verify_body

API_URL = os.environ.get(
    "PAYMENT_API_URL",
    "http://localhost:8000",
)
API_KEY = os.environ.get(
    "PAYMENT_API_KEY",
    "local-dev-key",
)
WEBHOOK_SECRET = os.environ.get(
    "PAYMENT_WEBHOOK_SECRET",
    "local-webhook-secret",
)
WEBHOOK_PORT = int(
    os.environ.get(
        "PAYMENT_E2E_WEBHOOK_PORT",
        "9000",
    )
)


class WebhookCollector:
    """Минимальный HTTP-сервер: собирает сырые байты тела и заголовок подписи.

    Порт 9000 общий для всех webhook-сценариев стенда (в него же попадают
    ретраи уведомлений других прогонов), поэтому забирать сообщения нужно по
    id своего платежа, а не по порядку прихода.
    """

    def __init__(self) -> None:
        self.received: list[tuple[str | None, bytes]] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                body = self.rfile.read(int(self.headers["Content-Length"]))
                server.received.append(
                    (
                        self.headers.get("X-Signature"),
                        body,
                    )
                )
                self.send_response(200)
                self.end_headers()

            def log_message(
                self,
                format: str,
                *args: object,
            ) -> None:  # noqa: A002
                # тишина в логах pytest
                pass

        self.httpd = HTTPServer(
            (
                "0.0.0.0",
                WEBHOOK_PORT,
            ),
            Handler,
        )
        self.thread = threading.Thread(
            target=self.httpd.serve_forever,
            daemon=True,
        )

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    @staticmethod
    def docker_host_url() -> str:
        # consumer работает внутри контейнера: адрес хоста — host.docker.internal
        host = os.environ.get(
            "PAYMENT_WEBHOOK_PUBLIC_HOST",
            "host.docker.internal",
        )
        return f"http://{host}:{WEBHOOK_PORT}/webhook"

    @staticmethod
    def local_url() -> str:
        """Адрес коллектора для запросов из самого теста (без docker DNS)."""
        return f"http://127.0.0.1:{WEBHOOK_PORT}/webhook"

    def wait_for(
        self,
        *,
        payment_id: str,
        timeout: float = 15.0,
    ) -> tuple[str | None, bytes]:
        """Ждёт уведомление конкретного платежа, игнорируя чужие."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for signature, raw_body in list(self.received):
                try:
                    payload = json.loads(raw_body)
                except ValueError:
                    continue
                if (
                    isinstance(
                        payload,
                        dict,
                    )
                    and payload.get("id") == payment_id
                ):
                    return signature, raw_body
            time.sleep(0.2)
        seen = [json.loads(body).get("id") for _, body in list(self.received) if body]
        pytest.fail(
            f"webhook для платежа {payment_id} не дошёл за {timeout:.0f}с "
            f"(получено уведомлений: {len(self.received)}, id: {seen})"
        )


def headers(idempotency_key: str) -> dict:
    return {
        "X-API-Key": API_KEY,
        "Idempotency-Key": idempotency_key,
        "Content-Type": "application/json",
    }


def wait_for_final_status(
    client: httpx.Client,
    payment_id: str,
    timeout: float = 30.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(
            f"{API_URL}/api/v1/payments/{payment_id}",
            headers={"X-API-Key": API_KEY},
        )
        r.raise_for_status()
        payment = r.json()
        if payment["status"] != "pending":
            return payment
        time.sleep(0.5)
    pytest.fail("payment status did not leave 'pending' in time")


class TestFullFlow:
    def test_create_process_notify(self) -> None:
        collector = WebhookCollector()
        collector.start()
        key = f"e2e-{uuid.uuid4()}"
        payload_json = {
            "amount": "77.77",
            "currency": "RUB",
            "description": "E2E payment",
            "metadata": {"run": "pytest"},
            "webhook_url": collector.docker_host_url(),
        }
        try:
            with httpx.Client(timeout=10.0) as client:
                # 1. создание
                r = client.post(
                    f"{API_URL}/api/v1/payments",
                    headers=headers(key),
                    json=payload_json,
                )
                assert r.status_code == 202, r.text
                body = r.json()
                payment_id = body["payment_id"]
                assert body["status"] == "pending"
                assert "created_at" in body
                assert r.headers["Location"] == f"/api/v1/payments/{payment_id}"

                # 2. идемпотентный повтор — тот же платёж
                r2 = client.post(
                    f"{API_URL}/api/v1/payments",
                    headers=headers(key),
                    json=payload_json,
                )
                assert r2.status_code == 202
                assert r2.json()["payment_id"] == payment_id

                # 3. тот же ключ с другим телом — конфликт
                r3 = client.post(
                    f"{API_URL}/api/v1/payments",
                    headers=headers(key),
                    json={
                        **payload_json,
                        "amount": "99.99",
                        "currency": "USD",
                    },
                )
                assert r3.status_code == 409, r3.text

                # 4. обработка consumer'ом: 2-5 сек
                payment = wait_for_final_status(
                    client,
                    payment_id,
                )
                assert payment["status"] in (
                    "succeeded",
                    "failed",
                )
                assert payment["processed_at"] is not None
                if payment["status"] == "failed":
                    assert payment["failure_reason"]

                # 5. webhook дошёл и подпись валидна для СЫРЫХ байтов тела
                signature, raw_body = collector.wait_for(
                    payment_id=payment_id,
                    timeout=15.0,
                )
                assert signature is not None
                verified = verify_body(
                    WEBHOOK_SECRET,
                    raw_body,
                    signature,
                )
                assert verified, "signature does not match the delivered body"
                webhook_payload = json.loads(raw_body)
                assert webhook_payload["id"] == payment_id
                assert webhook_payload["status"] == payment["status"]
                # приватные данные не утекают в webhook
                assert "webhook_url" not in webhook_payload
                assert "metadata" not in webhook_payload
        finally:
            collector.stop()

    def test_foreign_webhook_on_shared_port_does_not_break_the_scenario(self) -> None:
        """Ретрай чужого уведомления на том же порту не должен подменять проверку."""
        collector = WebhookCollector()
        collector.start()
        key = f"e2e-foreign-{uuid.uuid4()}"
        try:
            with httpx.Client(timeout=10.0) as client:
                foreign_id = str(uuid.uuid4())
                foreign = client.post(
                    collector.local_url(),
                    json={
                        "id": foreign_id,
                        "event": "payment.succeeded",
                        "event_id": "foreign",
                    },
                )
                assert foreign.status_code == 200

                payload_json = {
                    "amount": "5.55",
                    "currency": "EUR",
                    "description": "E2E payment with a foreign webhook",
                    "metadata": {},
                    "webhook_url": collector.docker_host_url(),
                }
                created = client.post(
                    f"{API_URL}/api/v1/payments",
                    headers=headers(key),
                    json=payload_json,
                )
                assert created.status_code == 202, created.text
                payment_id = created.json()["payment_id"]
                wait_for_final_status(
                    client,
                    payment_id,
                )

                signature, raw_body = collector.wait_for(
                    payment_id=payment_id,
                    timeout=20.0,
                )
                delivered = json.loads(raw_body)

                assert delivered["id"] == payment_id != foreign_id
                assert signature is not None
                verified = verify_body(
                    WEBHOOK_SECRET,
                    raw_body,
                    signature,
                )
                assert verified
        finally:
            collector.stop()

    def test_auth_required(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            r = client.get(f"{API_URL}/api/v1/payments/{uuid.uuid4()}")
            assert r.status_code == 401

    @pytest.mark.parametrize(
        (
            "headers",
            "expected_status",
        ),
        [
            (
                {},
                401,
            ),
            (
                {"X-API-Key": "wrong-key"},
                401,
            ),
            (
                {"X-API-Key": API_KEY},
                200,
            ),
        ],
    )
    def test_health_requires_api_key(
        self,
        headers: dict[str, str],
        expected_status: int,
    ) -> None:
        response = httpx.get(
            f"{API_URL}/health",
            headers=headers,
            timeout=10.0,
        )
        assert response.status_code == expected_status
        if expected_status == 200:
            assert response.json() == {"status": "ok"}

    def test_unknown_payment_404(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            r = client.get(
                f"{API_URL}/api/v1/payments/{uuid.uuid4()}",
                headers={"X-API-Key": API_KEY},
            )
            assert r.status_code == 404
