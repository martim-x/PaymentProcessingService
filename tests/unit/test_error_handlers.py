"""HTTP-маппинг доменных ошибок: клиентские ошибки — 4xx, не 500."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from payment.domain.exceptions import InvalidMoney, InvalidWebhookUrl
from payment.entrypoints.api.error_handlers import register_error_handlers


def build_app() -> FastAPI:
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/bad-money")
    async def bad_money() -> dict:
        raise InvalidMoney("-5", "amount must be positive")

    @app.get("/bad-webhook")
    async def bad_webhook() -> dict:
        raise InvalidWebhookUrl("ftp://example.com")

    return app


class TestDomainErrorMapping:
    def test_invalid_money_is_422(self) -> None:
        response = TestClient(build_app()).get("/bad-money")

        assert response.status_code == 422
        assert "amount must be positive" in response.json()["detail"]

    def test_invalid_webhook_url_is_422(self) -> None:
        response = TestClient(build_app()).get("/bad-webhook")

        assert response.status_code == 422
        assert "ftp://example.com" in response.json()["detail"]
