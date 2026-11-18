"""FastAPI app: сборка приложения и жизненный цикл."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payment.bootstrap import Container
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
        lifespan=lifespan,
    )
    app.state.container = container or Container()
    app.state.settings = app.state.container.settings
    app.state.create_payment = app.state.container.create_payment()
    app.state.get_payment = app.state.container.get_payment()

    app.include_router(payments_router)
    register_error_handlers(app)

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
