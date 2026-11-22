"""Регрессии входной валидации API и SSRF-гарда.

Проверяем настоящие маршруты роутера (не схемы в отрыве от HTTP): клиентские
ошибки обязаны давать 422 и валидный JSON, а не 500/падение сериализации.
"""

import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from payment.adapters.webhook.guard import UnsafeWebhookUrl, validate_public_http_url
from payment.application.services.payment_service import PaymentService
from payment.domain.exceptions import InvalidWebhookUrl
from payment.domain.model.value_objects import WebhookUrl
from payment.entrypoints.api.error_handlers import register_error_handlers
from payment.entrypoints.api.routers.payments import router
from tests.fakes import FakeGateway, FakeUnitOfWork

pytestmark = [
    pytest.mark.unit,
    pytest.mark.entrypoints,
]


API_KEY = "test-api-key"
VALID_BODY = {
    "amount": "10.00",
    "currency": "RUB",
    "description": "order 42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://example.com/hook",
}


class _RecordingPaymentService:
    """Настоящий сервис на fake-репозиториях: доменная валидация участвует."""

    def __init__(self) -> None:
        self._inner = PaymentService(
            uow=FakeUnitOfWork(),
            gateway=FakeGateway(),
        )
        self.calls: list = []

    async def create_payment(
        self,
        *,
        command,
    ):
        self.calls.append(command)
        return await self._inner.create_payment(command=command)


class _FakeContainer:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(api_key=API_KEY)
        self.payment_service_instance = _RecordingPaymentService()

    def payment_service(self) -> _RecordingPaymentService:
        return self.payment_service_instance


@pytest.fixture()
def container() -> _FakeContainer:
    return _FakeContainer()


@pytest.fixture()
def client(container: _FakeContainer) -> TestClient:
    """Реальное приложение с реальным роутером и обработчиками ошибок.

    raise_server_exceptions=False — дефект должен вернуться как HTTP-ответ,
    иначе неотловленное исключение маскирует его под падение теста.
    """
    app = FastAPI()
    app.state.settings = container.settings
    app.state.container = container
    app.include_router(router)
    register_error_handlers(app)
    return TestClient(
        app,
        raise_server_exceptions=False,
    )


def _post(
    client: TestClient,
    *,
    key: str | None = "key-1",
    body: dict | None = None,
):
    headers = {"X-API-Key": API_KEY}
    if key is not None:
        headers["Idempotency-Key"] = key
    return client.post(
        "/api/v1/payments",
        json=VALID_BODY if body is None else body,
        headers=headers,
    )


def _post_raw(
    client: TestClient,
    raw: str,
    *,
    key: str = "key-raw",
):
    return client.post(
        "/api/v1/payments",
        content=raw.encode("utf-8"),
        headers={
            "X-API-Key": API_KEY,
            "Idempotency-Key": key,
            "Content-Type": "application/json",
        },
    )


def _assert_422(response) -> dict:
    assert response.status_code == 422, (
        response.status_code,
        response.text,
    )
    # ответ обязан быть JSON: невалидный вход не должен ломать сериализацию
    return response.json()


class TestIdempotencyKeyHeader:
    def test_missing_key_is_422(
        self,
        client: TestClient,
    ) -> None:
        assert (
            _post(
                client,
                key=None,
            ).status_code
            == 422
        )

    def test_empty_key_is_422(
        self,
        client: TestClient,
    ) -> None:
        _assert_422(
            _post(
                client,
                key="",
            )
        )

    def test_blank_key_is_422(
        self,
        client: TestClient,
    ) -> None:
        _assert_422(
            _post(
                client,
                key="   ",
            )
        )

    def test_key_longer_than_255_is_422(
        self,
        client: TestClient,
    ) -> None:
        _assert_422(
            _post(
                client,
                key="x" * 256,
            )
        )

    def test_key_of_255_is_accepted(
        self,
        client: TestClient,
        container: _FakeContainer,
    ) -> None:
        response = _post(
            client,
            key="x" * 255,
        )

        assert response.status_code == 202, response.text
        assert len(container.payment_service().calls) == 1

    def test_invalid_key_never_reaches_service(
        self,
        client: TestClient,
        container: _FakeContainer,
    ) -> None:
        _post(
            client,
            key="   ",
        )
        _post(
            client,
            key="x" * 256,
        )

        assert container.payment_service().calls == []


class TestNonFiniteNumbers:
    @pytest.mark.parametrize(
        "literal",
        [
            "NaN",
            "Infinity",
            "-Infinity",
        ],
    )
    def test_non_finite_amount_is_422_with_json_body(
        self,
        client: TestClient,
        literal: str,
    ) -> None:
        raw = json.dumps(VALID_BODY).replace(
            '"10.00"',
            literal,
        )

        error = _assert_422(
            _post_raw(
                client,
                raw,
            )
        )

        assert "detail" in error

    @pytest.mark.parametrize(
        "literal",
        [
            "NaN",
            "Infinity",
            "-Infinity",
        ],
    )
    def test_non_finite_metadata_value_is_422(
        self,
        client: TestClient,
        literal: str,
    ) -> None:
        raw = (
            '{"amount": "10.00", "currency": "RUB", "metadata": {"x": %s}, '
            '"webhook_url": "https://example.com/hook"}' % literal
        )

        _assert_422(
            _post_raw(
                client,
                raw,
            )
        )

    def test_non_finite_nested_metadata_is_422(
        self,
        client: TestClient,
    ) -> None:
        raw = (
            '{"amount": "10.00", "currency": "RUB", '
            '"metadata": {"items": [{"score": NaN}]}, '
            '"webhook_url": "https://example.com/hook"}'
        )

        _assert_422(
            _post_raw(
                client,
                raw,
            )
        )

    def test_non_finite_input_does_not_reach_service(
        self,
        client: TestClient,
        container: _FakeContainer,
    ) -> None:
        _post_raw(
            client,
            '{"amount": NaN, "currency": "RUB", "webhook_url": "https://example.com/hook"}',
        )

        assert container.payment_service().calls == []


class TestNulBytes:
    def test_nul_in_description_is_422(
        self,
        client: TestClient,
    ) -> None:
        body = dict(
            VALID_BODY,
            description="evil\x00text",
        )

        _assert_422(
            _post(
                client,
                body=body,
            )
        )

    def test_nul_in_metadata_value_is_422(
        self,
        client: TestClient,
    ) -> None:
        body = dict(
            VALID_BODY,
            metadata={"note": "evil\x00text"},
        )

        _assert_422(
            _post(
                client,
                body=body,
            )
        )

    def test_nul_in_metadata_key_is_422(
        self,
        client: TestClient,
    ) -> None:
        body = dict(
            VALID_BODY,
            metadata={"evil\x00key": "value"},
        )

        _assert_422(
            _post(
                client,
                body=body,
            )
        )

    def test_nul_in_nested_metadata_is_422(
        self,
        client: TestClient,
    ) -> None:
        body = dict(
            VALID_BODY,
            metadata={"items": [{"note": "a\x00b"}]},
        )

        _assert_422(
            _post(
                client,
                body=body,
            )
        )

    def test_nul_input_does_not_reach_service(
        self,
        client: TestClient,
        container: _FakeContainer,
    ) -> None:
        _post(
            client,
            body=dict(
                VALID_BODY,
                description="a\x00b",
            ),
        )

        assert container.payment_service().calls == []


class TestValidRequestStillWorks:
    def test_valid_payment_is_202(
        self,
        client: TestClient,
        container: _FakeContainer,
    ) -> None:
        response = _post(client)

        assert response.status_code == 202, response.text
        assert response.json()["status"] == "pending"
        assert len(container.payment_service().calls) == 1

    def test_unicode_metadata_is_not_rejected(
        self,
        client: TestClient,
    ) -> None:
        body = dict(
            VALID_BODY,
            description="оплата",
            metadata={"note": "тест"},
        )

        assert (
            _post(
                client,
                body=body,
            ).status_code
            == 202
        )


class TestNonGlobalAddresses:
    """100.64.0.0/10 (CGNAT) не является private/reserved, но и не глобальный."""

    @pytest.mark.parametrize(
        "url",
        [
            "http://224.0.0.1/hook",
            "http://[ff02::1]/hook",
        ],
    )
    def test_multicast_is_rejected_by_both_guards(
        self,
        url: str,
    ) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url(url)
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl(value=url)

    @pytest.mark.parametrize(
        "url",
        [
            "http://100.64.0.1/hook",
            "http://100.127.255.255/hook",
        ],
    )
    def test_guard_rejects_cgnat_address(
        self,
        url: str,
    ) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url(url)

    def test_guard_still_allows_global_literal_ip(self) -> None:
        validate_public_http_url("https://93.184.216.34/hook")

    @pytest.mark.parametrize(
        "url",
        [
            "http://100.64.0.1/hook",
            "http://100.127.255.255/hook",
        ],
    )
    def test_webhook_value_object_rejects_cgnat_address(
        self,
        url: str,
    ) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl(value=url)

    def test_api_rejects_cgnat_webhook_url(
        self,
        client: TestClient,
    ) -> None:
        body = dict(
            VALID_BODY,
            webhook_url="http://100.64.0.1/hook",
        )

        _assert_422(
            _post(
                client,
                body=body,
            )
        )
