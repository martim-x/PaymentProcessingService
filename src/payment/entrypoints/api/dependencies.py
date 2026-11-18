"""Dependencies FastAPI: X-API-Key и сборка use case'ов."""

from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from payment.application.use_cases.create_payment import CreatePayment
from payment.application.use_cases.get_payment import GetPayment
from payment.config import Settings


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


async def require_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    settings: Annotated[Settings, Depends(get_settings)] = None,  # type: ignore[assignment]
) -> None:
    if x_api_key is None or x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing X-API-Key"
        )


def build_create_payment(request: Request) -> CreatePayment:
    return request.app.state.create_payment


def build_get_payment(request: Request) -> GetPayment:
    return request.app.state.get_payment
