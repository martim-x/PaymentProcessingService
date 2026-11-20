"""Роутер /api/v1/payments."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status

from payment.application.dto import CreatePaymentCommand, PaymentDetails
from payment.application.services.payment_service import PaymentService
from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import IdempotencyKey, PaymentId, to_decimal
from payment.entrypoints.api.dependencies import (
    build_payment_service,
    require_api_key,
    require_idempotency_key,
)
from payment.entrypoints.api.schemas import (
    CreatePaymentRequest,
    PaymentAcceptedResponse,
    PaymentDetailResponse,
)

router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)]
)


def _command(body: CreatePaymentRequest, idempotency_key: str) -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=to_decimal(body.amount),
        currency=Currency(body.currency),
        description=body.description,
        metadata=dict(body.metadata),
        idempotency_key=IdempotencyKey(idempotency_key),
        webhook_url=body.webhook_url,
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAcceptedResponse,
    summary="Создать платёж (асинхронная обработка)",
)
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
    payment_service: Annotated[PaymentService, Depends(build_payment_service)],
    response: Response,
) -> PaymentAcceptedResponse:
    result = await payment_service.create_payment(_command(body, idempotency_key))
    if result.created:
        response.headers["Location"] = f"/api/v1/payments/{result.payment.id}"
    return PaymentAcceptedResponse(
        payment_id=result.payment.id.value,
        status=result.payment.status.value,
        created_at=result.payment.created_at,
    )


@router.get("/{payment_id}", response_model=PaymentDetailResponse)
async def get_payment(
    payment_id: str,
    payment_service: Annotated[PaymentService, Depends(build_payment_service)],
) -> PaymentDetails:
    try:
        pid = PaymentId.from_string(payment_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        ) from exc
    return await payment_service.get_payment(pid)
