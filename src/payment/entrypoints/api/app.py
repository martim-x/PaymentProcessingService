"""FastAPI app: сборка приложения и жизненный цикл."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse

from payment.bootstrap import Container
from payment.entrypoints.api.dependencies import require_api_key
from payment.entrypoints.api.error_handlers import register_error_handlers
from payment.entrypoints.api.routers.payments import router as payments_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    container: Container = app.state.container
    yield
    await container.dispose()


def create_app(container: Container | None = None) -> FastAPI:
    app = FastAPI(
        title="Payment Processing Service",
        version="0.1.0",
        description="Асинхронный сервис процессинга платежей (тестовое задание)",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.container = container or Container()
    app.state.settings = app.state.container.settings
    # сервисы создаются НА ЗАПРОС: UoW внутри держит сессию, и общий
    # экземпляр на всё приложение смешал бы транзакции параллельных запросов

    app.include_router(payments_router)
    register_error_handlers(app)

    @app.get("/health", tags=["system"], dependencies=[Depends(require_api_key)])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(
        "/api/v1/openapi.json",
        tags=["system"],
        dependencies=[Depends(require_api_key)],
        include_in_schema=False,
    )
    async def openapi_schema() -> JSONResponse:
        """Схема API под тем же ключом, что и остальные эндпоинты.

        Swagger UI, Redoc и публичный `/openapi.json` отключены: ТЗ требует
        `X-API-Key` на всех эндпоинтах, а страницы документации FastAPI
        регистрирует без авторизации. Схему забирают curl'ом с заголовком и
        открывают в Swagger Editor или `redocly preview-docs`.
        """
        return JSONResponse(app.openapi())

    return app


app = create_app()
