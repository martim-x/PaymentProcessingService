"""Маппинг application-исключений в HTTP-коды."""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from payment.application.exceptions import ApplicationError, IdempotencyConflict, PaymentNotFound


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFound)
    async def _payment_not_found(request: Request, exc: PaymentNotFound) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc)},
        )

    @app.exception_handler(IdempotencyConflict)
    async def _idempotency_conflict(request: Request, exc: IdempotencyConflict) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(exc)},
        )

    @app.exception_handler(ApplicationError)
    async def _application_error(request: Request, exc: ApplicationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": str(exc)},
        )
