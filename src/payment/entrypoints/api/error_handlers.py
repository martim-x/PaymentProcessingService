"""Маппинг доменных и application-исключений в HTTP-коды."""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from payment.application.exceptions import ApplicationError, IdempotencyConflict, PaymentNotFound
from payment.domain.exceptions import (
    DomainError,
    InvalidMoney,
    InvalidStatusTransition,
    InvalidWebhookUrl,
)


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFound)
    async def _payment_not_found(request: Request, exc: PaymentNotFound) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})

    @app.exception_handler(IdempotencyConflict)
    async def _idempotency_conflict(request: Request, exc: IdempotencyConflict) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)})

    @app.exception_handler(InvalidStatusTransition)
    async def _invalid_transition(request: Request, exc: InvalidStatusTransition) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)})

    # ошибки валидации домена — это ошибка клиента, а не сервера
    @app.exception_handler(InvalidMoney)
    @app.exception_handler(InvalidWebhookUrl)
    async def _domain_validation_error(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": str(exc)}
        )

    @app.exception_handler(ApplicationError)
    async def _application_error(request: Request, exc: ApplicationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": str(exc)}
        )
