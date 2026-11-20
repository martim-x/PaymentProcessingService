"""Жизненный цикл UoW: сервис создаётся на запрос, а не на приложение."""

import os
import uuid
from types import SimpleNamespace

os.environ.setdefault("PAYMENT_API_KEY", "safe-test-api-key")
os.environ.setdefault("PAYMENT_WEBHOOK_SECRET", "safe-test-webhook-secret")

import pytest
from fastapi.testclient import TestClient

from payment.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from payment.entrypoints.api.dependencies import build_payment_service
from payment.entrypoints.api.routers.payments import _command
from payment.entrypoints.api.schemas import CreatePaymentRequest


class CountingContainer:
    """Минимальный контейнер: считает, сколько сервисов создано."""

    def __init__(self) -> None:
        self.created = []

    def payment_service(self):
        service = object()
        self.created.append(service)
        return service


def fake_request(container: CountingContainer) -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


class TestPerRequestService:
    def test_each_request_gets_its_own_service(self) -> None:
        container = CountingContainer()
        request = fake_request(container)

        first = build_payment_service(request)
        second = build_payment_service(request)
        third = build_payment_service(request)

        assert first is not second
        assert len(container.created) == 3
        assert third is container.created[2]


class TestApiRoutesAuthentication:
    def test_docs_endpoints_are_disabled_and_api_auth_is_unchanged(self) -> None:
        import sys

        sys.modules.pop("payment.entrypoints.api.app", None)
        from payment.entrypoints.api.app import create_app

        settings = SimpleNamespace(api_key="test-api-key")
        container = SimpleNamespace(settings=settings, payment_service=lambda: object())
        app = create_app(container)  # type: ignore[arg-type]
        client = TestClient(app)

        for path in ("/docs", "/redoc", "/openapi.json"):
            assert client.get(path).status_code == 404

        assert client.get("/health").status_code == 401
        assert client.get("/health", headers={"X-API-Key": "wrong-key"}).status_code == 401
        assert client.get("/health", headers={"X-API-Key": "test-api-key"}).status_code == 200
        assert (
            client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000").status_code == 401
        )
        assert (
            client.get(
                "/api/v1/payments/not-a-uuid", headers={"X-API-Key": "test-api-key"}
            ).status_code
            == 404
        )

    def test_openapi_schema_is_served_only_with_the_api_key(self) -> None:
        """Схема доступна без Swagger UI, но только под X-API-Key."""
        import sys

        sys.modules.pop("payment.entrypoints.api.app", None)
        from payment.entrypoints.api.app import create_app

        settings = SimpleNamespace(api_key="test-api-key")
        container = SimpleNamespace(settings=settings, payment_service=lambda: object())
        client = TestClient(create_app(container))  # type: ignore[arg-type]

        assert client.get("/api/v1/openapi.json").status_code == 401
        assert (
            client.get("/api/v1/openapi.json", headers={"X-API-Key": "wrong-key"}).status_code
            == 401
        )

        response = client.get("/api/v1/openapi.json", headers={"X-API-Key": "test-api-key"})

        assert response.status_code == 200
        schema = response.json()
        assert "/api/v1/payments" in schema["paths"]
        assert "/health" in schema["paths"]


class TestUnitOfWorkReentryGuard:
    """UoW хранит сессию: повторный вход должен падать, а не смешивать транзакции."""

    class _FakeSession:
        async def close(self) -> None:
            pass

    @staticmethod
    def _uow() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(lambda: TestUnitOfWorkReentryGuard._FakeSession())

    async def test_second_enter_raises(self) -> None:
        uow = self._uow()
        async with uow:
            with pytest.raises(RuntimeError, match="already in use"):
                async with uow:
                    pass

    async def test_can_be_reused_after_exit(self) -> None:
        uow = self._uow()
        async with uow:
            pass
        async with uow:  # последовательно — можно
            pass


class TestCommandBuilding:
    def test_fingerprint_is_not_part_of_command(self) -> None:
        body = CreatePaymentRequest(
            amount="100.50",
            currency="RUB",
            description="d",
            metadata={},
            webhook_url="https://example.com/hook",
        )

        command = _command(body, f"key-{uuid.uuid4()}")

        assert not hasattr(command, "fingerprint")
        assert command.webhook_url == "https://example.com/hook"
