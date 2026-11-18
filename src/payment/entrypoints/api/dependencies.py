"""Dependencies FastAPI: X-API-Key и сборка use case'ов."""

import hmac
from typing import Annotated

from fastapi import Header, HTTPException, Request, status

from payment.application.use_cases.create_payment import CreatePayment
from payment.application.use_cases.get_payment import GetPayment
from payment.config import Settings


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


async def require_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    settings: Settings = request.app.state.settings
    provided = (x_api_key or "").encode("utf-8")
    expected = settings.api_key.encode("utf-8")
    # compare_digest — константное время сравнения, без утечки по таймингу
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing X-API-Key"
        )


def build_create_payment(request: Request) -> CreatePayment:
    """Новый use case на каждый запрос: свой UoW и своя сессия."""
    return request.app.state.container.create_payment()


def build_get_payment(request: Request) -> GetPayment:
    """Новый use case на каждый запрос: свой UoW и своя сессия."""
    return request.app.state.container.get_payment()


__all__ = ["get_settings", "require_api_key", "build_create_payment", "build_get_payment"]
