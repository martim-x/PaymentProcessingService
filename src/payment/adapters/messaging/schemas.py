"""Формат сообщения в брокере (Pydantic-схема payload)."""

from pydantic import BaseModel, Field


class PaymentCreatedMessage(BaseModel):
    event: str = Field(default="payment.created")
    payment_id: str
