"""FastAPI app: сборка приложения и жизненный цикл."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from payment.bootstrap import Container
from payment.entrypoints.api.dependencies import RequestDependencies
from payment.entrypoints.api.error_handlers import ApiErrorHandlers
from payment.entrypoints.api.routers.payments import PaymentRoutes


class PaymentApi:
    @staticmethod
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        container: Container = app.state.container
        yield
        await container.dispose()

    @classmethod
    def create_app(cls, container: Container | None = None) -> FastAPI:
        app = FastAPI(
            title="Payment Processing Service",
            version="0.1.0",
            description="Асинхронный сервис процессинга платежей (тестовое задание)",
            docs_url=None,
            redoc_url=None,
            openapi_url=None,
            lifespan=cls.lifespan,
        )
        app.state.container = container or Container()
        app.state.settings = app.state.container.settings
        app.include_router(PaymentRoutes.build_router())
        ApiErrorHandlers.register(app)
        app.get(
            "/health",
            tags=["system"],
            dependencies=[Depends(RequestDependencies.require_api_key)],
        )(cls.health)
        app.get(
            "/api/v1/openapi.json",
            tags=["system"],
            dependencies=[Depends(RequestDependencies.require_api_key)],
            include_in_schema=False,
        )(cls.openapi_schema)
        return app

    @staticmethod
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @staticmethod
    async def openapi_schema(request: Request) -> JSONResponse:
        """Возвращает OpenAPI только после проверки API-ключа."""
        return JSONResponse(request.app.openapi())


app = PaymentApi.create_app()
