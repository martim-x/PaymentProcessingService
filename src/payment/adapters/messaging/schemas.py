"""Формат сообщений в брокере (Pydantic-схемы payload)."""

from pydantic import BaseModel, Field

from payment.application.event_types import OutboxEventType


class PaymentCreatedMessage(BaseModel):
    """Формат сообщения о создании платежа в брокере."""

    event: str = Field(default=OutboxEventType.PAYMENT_CREATED.value)
    payment_id: str
