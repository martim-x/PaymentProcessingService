"""Маппинг доменных и application-исключений в HTTP-коды."""

import math
from decimal import Decimal
from typing import Any, cast

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from payment.application.exceptions import (
    ApplicationError,
    IdempotencyConflict,
    PaymentNotFound,
)
from payment.domain.exceptions import (
    DomainError,
    InvalidMoney,
    InvalidStatusTransition,
    InvalidWebhookUrl,
)


class ApiErrorHandlers:
    @classmethod
    def register(cls, app: FastAPI) -> None:
        app.exception_handler(RequestValidationError)(cls._request_validation_error)
        app.exception_handler(PaymentNotFound)(cls._payment_not_found)
        app.exception_handler(IdempotencyConflict)(cls._idempotency_conflict)
        app.exception_handler(InvalidStatusTransition)(cls._invalid_transition)
        app.exception_handler(InvalidMoney)(cls._domain_validation_error)
        app.exception_handler(InvalidWebhookUrl)(cls._domain_validation_error)
        app.exception_handler(ApplicationError)(cls._application_error)

    @staticmethod
    def _json_safe(value: Any) -> Any:
        """Заменяет non-finite числа строками до JSON-сериализации.

        Pydantic кладёт исходный ввод в detail ошибки: Decimal('NaN')/float('nan')
        не кодируются json.dumps, и клиент вместо 422 получал бы 500.
        """
        if isinstance(
            value,
            float,
        ) and not math.isfinite(value):
            return str(value)
        if (
            isinstance(
                value,
                Decimal,
            )
            and not value.is_finite()
        ):
            return str(value)
        if isinstance(
            value,
            dict,
        ):
            typed_value = cast(
                dict[Any, Any],
                value,
            )
            return {
                str(key): ApiErrorHandlers._json_safe(item)
                for key, item in typed_value.items()
            }
        if isinstance(
            value,
            (
                list,
                tuple,
            ),
        ):
            sequence_value = cast(
                list[Any] | tuple[Any, ...],
                value,
            )
            return [ApiErrorHandlers._json_safe(item) for item in sequence_value]
        return value

    @staticmethod
    async def _request_validation_error(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={
                "detail": jsonable_encoder(ApiErrorHandlers._json_safe(exc.errors()))
            },
        )

    @staticmethod
    async def _payment_not_found(
        request: Request,
        exc: PaymentNotFound,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc)},
        )

    @staticmethod
    async def _idempotency_conflict(
        request: Request,
        exc: IdempotencyConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(exc)},
        )

    @staticmethod
    async def _invalid_transition(
        request: Request,
        exc: InvalidStatusTransition,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(exc)},
        )

    # ошибки валидации домена — это ошибка клиента, а не сервера
    @staticmethod
    async def _domain_validation_error(
        request: Request,
        exc: DomainError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"detail": str(exc)},
        )

    @staticmethod
    async def _application_error(
        request: Request,
        exc: ApplicationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": str(exc)},
        )
