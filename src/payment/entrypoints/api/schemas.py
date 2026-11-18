"""Pydantic v2-схемы API."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

CurrencyLiteral = Literal["RUB", "USD", "EUR"]

MAX_METADATA_BYTES = 8192


class CreatePaymentRequest(BaseModel):
    amount: Decimal = Field(gt=0, examples=["100.00"])
    currency: CurrencyLiteral
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str = Field(max_length=2048, examples=["https://example.com/webhook"])

    @field_validator("metadata")
    @classmethod
    def _metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(value, ensure_ascii=False).encode("utf-8")
        if len(encoded) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must be at most {MAX_METADATA_BYTES} bytes")
        return value


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
    failure_reason: str | None = None
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None = None
