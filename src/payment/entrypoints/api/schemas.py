"""Pydantic v2-схемы API."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

CurrencyLiteral = Literal["RUB", "USD", "EUR"]


class CreatePaymentRequest(BaseModel):
    amount: Decimal = Field(gt=0, examples=["100.00"])
    currency: CurrencyLiteral
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str = Field(examples=["https://example.com/webhook"])


class PaymentAcceptedResponse(BaseModel):
    payment_id: UUID
    status: str
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any]
    status: str
    failure_reason: Optional[str] = None
    webhook_url: str
    created_at: datetime
    processed_at: Optional[datetime] = None
