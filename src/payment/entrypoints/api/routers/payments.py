"""Роутер /api/v1/payments."""

from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from payment.application.dto import CreatePaymentCommand, PaymentDetails
from payment.application.use_cases.create_payment import CreatePayment
from payment.application.use_cases.get_payment import GetPayment
from payment.domain.model.enums import Currency
from payment.domain.model.fingerprint import compute_fingerprint
from payment.domain.model.value_objects import IdempotencyKey, PaymentId, to_decimal
from payment.entrypoints.api.dependencies import (
    build_create_payment,
    build_get_payment,
    require_api_key,
)
from payment.entrypoints.api.schemas import (
    CreatePaymentRequest,
    PaymentAcceptedResponse,
    PaymentDetailResponse,
)

router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)]
)


def _canonical_amount(amount: Decimal) -> str:
    """'100.5' и '100.50' должны давать один fingerprint (одна и та же сумма)."""
    return format(amount.normalize(), "f")


def _command(body: CreatePaymentRequest, idempotency_key: str) -> CreatePaymentCommand:
    amount = to_decimal(body.amount)
    raw = {
        "amount": _canonical_amount(amount),
        "currency": body.currency,
        "description": body.description,
        "metadata": body.metadata,
        "webhook_url": body.webhook_url,
    }
    return CreatePaymentCommand(
        amount=amount,
        currency=Currency(body.currency),
        description=body.description,
        metadata=dict(body.metadata),
        idempotency_key=IdempotencyKey(idempotency_key),
        webhook_url=body.webhook_url,
        fingerprint=compute_fingerprint(raw),
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAcceptedResponse,
    summary="Создать платёж (асинхронная обработка)",
)
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[str, Header()],
    use_case: Annotated[CreatePayment, Depends(build_create_payment)],
    response: Response,
) -> PaymentAcceptedResponse:
    result = await use_case.execute(_command(body, idempotency_key))
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
    use_case: Annotated[GetPayment, Depends(build_get_payment)],
) -> PaymentDetails:
    try:
        pid = PaymentId.from_string(payment_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        ) from exc
    return await use_case.execute(pid)
