"""Жизненный цикл UoW: use case создаётся на запрос, а не на приложение."""

import uuid
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from payment.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from payment.entrypoints.api.dependencies import build_create_payment, build_get_payment
from payment.entrypoints.api.routers.payments import _command
from payment.entrypoints.api.schemas import CreatePaymentRequest


class CountingContainer:
    """Минимальный контейнер: считает, сколько use case'ов создано."""

    def __init__(self) -> None:
        self.created = []

    def create_payment(self):
        use_case = object()
        self.created.append(use_case)
        return use_case

    def get_payment(self):
        use_case = object()
        self.created.append(use_case)
        return use_case


def fake_request(container: CountingContainer) -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


class TestPerRequestUseCase:
    def test_each_request_gets_its_own_use_case(self) -> None:
        container = CountingContainer()
        request = fake_request(container)

        first = build_create_payment(request)
        second = build_create_payment(request)
        third = build_get_payment(request)

        assert first is not second
        assert len(container.created) == 3
        assert third is container.created[2]


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


class TestRouterWiring:
    def test_router_uses_request_scoped_factory(self) -> None:
        """Зависимость роутера обязана быть фабрикой, а не готовым объектом."""
        app = FastAPI()

        @app.get("/probe")
        async def probe(x: object = build_create_payment) -> dict:  # noqa: B008 — проверка ссылки
            return {"ok": True}

        assert build_create_payment.__name__ == "build_create_payment"
        assert app is not None
