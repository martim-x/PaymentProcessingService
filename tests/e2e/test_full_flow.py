"""E2E: полный сценарий против работающего docker compose стека.

Запуск локально:  docker compose up --build -d  &&  pytest tests/e2e -q
В CI: поднимается job'ом e2e (docker compose up --build -d --wait).

Требует: PAYMENT_API_URL (по умолчанию http://localhost:8000), PAYMENT_API_KEY.
"""

import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest

API_URL = os.environ.get("PAYMENT_API_URL", "http://localhost:8000")
API_KEY = os.environ.get("PAYMENT_API_KEY", "local-dev-key")
WEBHOOK_PORT = int(os.environ.get("PAYMENT_E2E_WEBHOOK_PORT", "9000"))


class WebhookCollector:
    """Минимальный HTTP-сервер, собирающий webhook-уведомления в список."""

    def __init__(self) -> None:
        self.received: list[tuple[str | None, dict]] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                body = self.rfile.read(int(self.headers["Content-Length"]))
                import json

                server.received.append((self.headers.get("X-Signature"), json.loads(body)))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args) -> None:  # тишина в логах pytest
                pass

        self.httpd = HTTPServer(("0.0.0.0", WEBHOOK_PORT), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()

    @staticmethod
    def docker_host_url() -> str:
        # consumer работает внутри контейнера: адрес хоста должен быть
        # host.docker.internal (на хосте-маке он не резолвится, поэтому без проверки DNS)
        host = os.environ.get("PAYMENT_WEBHOOK_PUBLIC_HOST", "host.docker.internal")
        return f"http://{host}:{WEBHOOK_PORT}/webhook"


def headers(idempotency_key: str) -> dict:
    return {
        "X-API-Key": API_KEY,
        "Idempotency-Key": idempotency_key,
        "Content-Type": "application/json",
    }


def wait_for_final_status(client: httpx.Client, payment_id: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"{API_URL}/api/v1/payments/{payment_id}", headers={"X-API-Key": API_KEY})
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
        try:
            with httpx.Client(timeout=10.0) as client:
                # 1. создание
                r = client.post(
                    f"{API_URL}/api/v1/payments",
                    headers=headers(key),
                    json={
                        "amount": "77.77",
                        "currency": "RUB",
                        "description": "E2E payment",
                        "metadata": {"run": "pytest"},
                        "webhook_url": collector.docker_host_url(),
                    },
                )
                assert r.status_code == 202, r.text
                body = r.json()
                payment_id = body["payment_id"]
                assert body["status"] == "pending"
                assert "created_at" in body

                # 2. идемпотентный повтор — тот же платёж
                r2 = client.post(
                    f"{API_URL}/api/v1/payments",
                    headers=headers(key),
                    json={
                        "amount": "77.77",
                        "currency": "RUB",
                        "description": "E2E payment",
                        "metadata": {"run": "pytest"},
                        "webhook_url": collector.docker_host_url(),
                    },
                )
                assert r2.status_code == 202
                assert r2.json()["payment_id"] == payment_id

                # 3. тот же ключ с другим телом — конфликт
                r3 = client.post(
                    f"{API_URL}/api/v1/payments",
                    headers=headers(key),
                    json={
                        "amount": "99.99",
                        "currency": "USD",
                        "description": "E2E conflict",
                        "metadata": {},
                        "webhook_url": collector.docker_host_url(),
                    },
                )
                assert r3.status_code == 409, r3.text

                # 4. обработка consumer'ом: 2-5 сек
                payment = wait_for_final_status(client, payment_id)
                assert payment["status"] in ("succeeded", "failed")
                assert payment["processed_at"] is not None
                if payment["status"] == "failed":
                    assert payment["failure_reason"]

                # 5. webhook дошёл и подписан
                deadline = time.monotonic() + 15.0
                while time.monotonic() < deadline and not collector.received:
                    time.sleep(0.3)
                assert collector.received, "webhook was not delivered"
                signature, payload = collector.received[0]
                assert signature is not None and signature.startswith("sha256=")
                assert payload["id"] == payment_id
                assert payload["status"] == payment["status"]
        finally:
            collector.stop()

    def test_auth_required(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            r = client.get(f"{API_URL}/api/v1/payments/{uuid.uuid4()}")
            assert r.status_code == 401

    def test_unknown_payment_404(self) -> None:
        with httpx.Client(timeout=10.0) as client:
            r = client.get(
                f"{API_URL}/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": API_KEY}
            )
            assert r.status_code == 404
