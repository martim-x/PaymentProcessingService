"""Dependencies FastAPI: X-API-Key и сборка сервисов на запрос."""

import hmac
from typing import Annotated

from fastapi import Header, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError

from payment.application.services.payment_service import PaymentService
from payment.config import Settings
from payment.domain.model.value_objects import IdempotencyKey


class RequestDependencies:
    @staticmethod
    async def require_api_key(
        request: Request,
        x_api_key: Annotated[str | None, Header()] = None,
    ) -> None:
        settings: Settings = request.app.state.settings
        provided = (x_api_key or "").encode("utf-8")
        expected = settings.api_key.encode("utf-8")
        # compare_digest — константное время сравнения, без утечки по таймингу
        if not provided or not hmac.compare_digest(
            provided,
            expected,
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing X-API-Key",
            )

    @staticmethod
    def _idempotency_key_error(message: str) -> RequestValidationError:
        """Ошибка заголовка в формате FastAPI-валидации (422), а не 500 из домена."""
        return RequestValidationError(
            [
                {
                    "type": "value_error",
                    "loc": (
                        "header",
                        "idempotency-key",
                    ),
                    "msg": message,
                    "input": None,
                }
            ]
        )

    @staticmethod
    async def require_idempotency_key(
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> str:
        """Заголовок обязателен, не пуст/не blank и не длиннее VARCHAR(255)."""
        if not idempotency_key.strip():
            raise RequestDependencies._idempotency_key_error(
                "Idempotency-Key must not be empty or blank"
            )
        if len(idempotency_key) > IdempotencyKey.MAX_LENGTH:
            raise RequestDependencies._idempotency_key_error(
                f"Idempotency-Key must be at most {IdempotencyKey.MAX_LENGTH} characters"
            )
        return idempotency_key

    @staticmethod
    def build_payment_service(request: Request) -> PaymentService:
        """Новый сервис на HTTP-запрос; вместе с ним создаётся свой UoW."""
        return request.app.state.container.payment_service()
