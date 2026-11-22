"""Pydantic v2-схемы API."""

import json
import math
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

CurrencyLiteral = Literal["RUB", "USD", "EUR"]


class CreatePaymentRequest(BaseModel):
    """Проверенная схема тела запроса на создание платежа."""

    _MAX_METADATA_BYTES: ClassVar[int] = 8192

    amount: Decimal = Field(
        gt=0,
        examples=["100.00"],
    )
    currency: CurrencyLiteral
    description: str = Field(
        default="",
        max_length=1000,
    )
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str = Field(
        max_length=2048,
        examples=["https://example.com/webhook"],
    )

    @staticmethod
    def _reject_nul(*, value: str, path: str) -> str:
        if "\x00" in value:
            raise ValueError(f"{path} must not contain NUL (\\u0000) characters")
        return value

    @classmethod
    def _check_json_value(cls, *, value: Any, path: str) -> None:
        """Отклоняет значения, которые PostgreSQL/JSON не примет или исказит."""
        if isinstance(value, str):
            cls._reject_nul(value=value, path=path)
        elif isinstance(value, (bool, int)) or value is None:
            return
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"{path} must be a finite number")
        elif isinstance(value, Decimal):
            if not value.is_finite():
                raise ValueError(f"{path} must be a finite number")
        elif isinstance(value, dict):
            for key, item in cast(dict[Any, Any], value).items():
                if not isinstance(key, str):
                    raise ValueError(f"{path} keys must be strings")
                cls._reject_nul(value=key, path=f"{path} key")
                cls._check_json_value(value=item, path=f"{path}.{key}")
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(cast(list[Any] | tuple[Any, ...], value)):
                cls._check_json_value(value=item, path=f"{path}[{index}]")

    @field_validator("description")
    @classmethod
    def _description_chars(
        cls,
        value: str,
    ) -> str:
        return cls._reject_nul(value=value, path="description")

    @field_validator("metadata")
    @classmethod
    def _metadata_size(
        cls,
        value: dict[str, Any],
    ) -> dict[str, Any]:
        cls._check_json_value(value=value, path="metadata")
        # allow_nan=False: невалидный JSON не должен попасть в БД/событие
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > cls._MAX_METADATA_BYTES:
            raise ValueError(
                f"metadata must be at most {cls._MAX_METADATA_BYTES} bytes"
            )
        return value


class PaymentAcceptedResponse(BaseModel):
    """Ответ API на принятый запрос создания платежа."""

    payment_id: UUID
    status: str
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    """Публичное представление платежа для API."""

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
