"""Формат сообщений в брокере (Pydantic-схемы payload)."""

from pydantic import BaseModel, Field


class PaymentCreatedMessage(BaseModel):
    """Формат сообщения о создании платежа в брокере."""

    event: str = Field(default="payment.created")
    payment_id: str
