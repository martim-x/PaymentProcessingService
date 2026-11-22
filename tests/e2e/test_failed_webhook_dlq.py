"""Live E2E: three failed webhook sends route the payment event to payments.dlq."""

import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest

API_URL = os.environ.get(
    "PAYMENT_API_URL",
    "http://localhost:8000",
)

pytestmark = pytest.mark.skipif(
    os.environ.get("PAYMENT_RUN_DLQ_E2E") != "1",
    reason="set PAYMENT_RUN_DLQ_E2E=1 to run the isolated DLQ E2E test",
)


def test_three_failed_webhooks_are_dead_lettered_with_stable_event_id() -> None:
    api_key = os.environ.get("PAYMENT_API_KEY")
    management_url = os.environ.get("PAYMENT_RABBITMQ_MANAGEMENT_URL")
    management_api_key = os.environ.get("PAYMENT_RABBITMQ_MANAGEMENT_API_KEY")
    port_value = os.environ.get("PAYMENT_E2E_WEBHOOK_PORT")
    if not api_key or not management_url or not management_api_key or not port_value:
        pytest.fail(
            "DLQ E2E requires PAYMENT_API_KEY, explicit "
            "PAYMENT_RABBITMQ_MANAGEMENT_URL, "
            "PAYMENT_RABBITMQ_MANAGEMENT_API_KEY, and PAYMENT_E2E_WEBHOOK_PORT"
        )
    webhook_port = int(port_value)
    if not 1 <= webhook_port <= 65535:
        pytest.fail("PAYMENT_E2E_WEBHOOK_PORT must be between 1 and 65535")
    if not management_url.startswith(
        (
            "http://",
            "https://",
        )
    ):
        pytest.fail("PAYMENT_RABBITMQ_MANAGEMENT_URL must be an explicit HTTP(S) URL")
    management_credentials = management_api_key.split(
        ":",
        1,
    )
    if len(management_credentials) != 2 or not all(management_credentials):
        pytest.fail(
            "PAYMENT_RABBITMQ_MANAGEMENT_API_KEY must be in username:password form"
        )
    calls: list[dict[str, object]] = []
    lock = threading.Lock()

    class FailingWebhook(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            body = self.rfile.read(int(self.headers["Content-Length"]))
            with lock:
                calls.append(json.loads(body))
            self.send_response(503)
            self.end_headers()

        def log_message(
            self,
            format: str,
            *args: object,
        ) -> None:  # noqa: A002
            pass

    server = HTTPServer(
        (
            "0.0.0.0",
            webhook_port,
        ),
        FailingWebhook,
    )
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )
    thread.start()
    payment_id: str | None = None
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(
                f"{API_URL}/api/v1/payments",
                headers={
                    "X-API-Key": api_key,
                    "Idempotency-Key": f"failed-hook-{uuid.uuid4()}",
                },
                json={
                    "amount": "17.25",
                    "currency": "RUB",
                    "description": "failed webhook DLQ E2E",
                    "metadata": {},
                    "webhook_url": f"http://host.docker.internal:{webhook_port}/webhook",
                },
            )
            assert response.status_code == 202, response.text
            payment_id = response.json()["payment_id"]

            deadline = time.monotonic() + 40
            payment: dict[str, object] = {}
            dlq_message: dict[str, object] | None = None
            while time.monotonic() < deadline:
                payment_response = client.get(
                    f"{API_URL}/api/v1/payments/{payment_id}",
                    headers={"X-API-Key": api_key},
                )
                payment_response.raise_for_status()
                payment = payment_response.json()
                # порт 9000 общий: рядом могут ретраиться уведомления других
                # сценариев, поэтому считаем только вызовы своего платежа
                own_calls = [entry for entry in calls if entry.get("id") == payment_id]
                if len(own_calls) >= 3:
                    fetched = client.post(
                        f"{management_url.rstrip('/')}/api/queues/%2F/payments.dlq/get",
                        auth=(
                            management_credentials[0],
                            management_credentials[1],
                        ),
                        json={
                            "count": 200,
                            "ackmode": "ack_requeue_true",
                            "encoding": "auto",
                            "truncate": 50000,
                        },
                    )
                    if fetched.status_code == 200 and fetched.json():
                        # В DLQ могут лежать сообщения прошлых прогонов: ищем своё,
                        # а не первое попавшееся.
                        for queued in fetched.json():
                            queued_payload = queued.get("payload")
                            if isinstance(
                                queued_payload,
                                str,
                            ):
                                try:
                                    queued_payload = json.loads(queued_payload)
                                except ValueError:
                                    continue
                            if (
                                isinstance(
                                    queued_payload,
                                    dict,
                                )
                                and queued_payload.get("payment_id") == payment_id
                            ):
                                dlq_message = queued
                                break
                        if dlq_message is not None:
                            break
                time.sleep(0.25)

            own_calls = [entry for entry in calls if entry.get("id") == payment_id]
            assert len(own_calls) == 3, (
                f"expected 3 failed webhook requests for {payment_id}, got {len(own_calls)} "
                f"(всего вызовов на общем порту: {len(calls)}). "
                "Если 0 — проверьте, что consumer работает с "
                "PAYMENT_WEBHOOK_ALLOW_PRIVATE_HOSTS=true: иначе SSRF-гард "
                "отклоняет host.docker.internal и HTTP-запроса не будет вовсе"
            )
            event_ids = [entry["event_id"] for entry in own_calls]
            assert (
                isinstance(
                    event_ids[0],
                    str,
                )
                and event_ids[0]
            )
            assert (
                event_ids == [event_ids[0]] * 3
            ), f"event_id changed across retries: {event_ids}"
            assert payment["status"] in {
                "succeeded",
                "failed",
            }, payment
            assert (
                dlq_message is not None
            ), "payment event did not appear in payments.dlq"
            dlq_payload = dlq_message.get("payload")
            if isinstance(
                dlq_payload,
                str,
            ):
                dlq_payload = json.loads(dlq_payload)
            assert isinstance(
                dlq_payload,
                dict,
            ), dlq_message
            assert dlq_payload.get("payment_id") == payment_id, dlq_payload
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
