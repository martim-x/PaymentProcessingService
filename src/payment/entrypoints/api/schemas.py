"""Pydantic v2-схемы API."""

import json
import math
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

CurrencyLiteral = Literal["RUB", "USD", "EUR"]

MAX_METADATA_BYTES = 8192
MAX_IDEMPOTENCY_KEY_LENGTH = 255
_NUL = "\x00"


def _reject_nul(
    value: str,
    path: str,
) -> str:
    if _NUL in value:
        raise ValueError(f"{path} must not contain NUL (\\u0000) characters")
    return value


def _check_json_value(
    value: Any,
    path: str,
) -> None:
    """Отклоняет значения, которые PostgreSQL/JSON не примет или исказит.

    NUL-байт в text/jsonb ломает вставку, а NaN/Infinity не являются
    валидным JSON и роняют сериализацию ответа об ошибке.
    """
    if isinstance(
        value,
        str,
    ):
        _reject_nul(
            value,
            path,
        )
    elif (
        isinstance(
            value,
            bool,
        )
        or value is None
        or isinstance(
            value,
            int,
        )
    ):
        return
    elif isinstance(
        value,
        float,
    ):
        if not math.isfinite(value):
            raise ValueError(f"{path} must be a finite number")
    elif isinstance(
        value,
        Decimal,
    ):
        if not value.is_finite():
            raise ValueError(f"{path} must be a finite number")
    elif isinstance(
        value,
        dict,
    ):
        typed_value = cast(
            dict[Any, Any],
            value,
        )
        for key, item in typed_value.items():
            if not isinstance(
                key,
                str,
            ):
                raise ValueError(f"{path} keys must be strings")
            _reject_nul(
                key,
                f"{path} key",
            )
            _check_json_value(
                item,
                f"{path}.{key}",
            )
    elif isinstance(
        value,
        (
            list,
            tuple,
        ),
    ):
        sequence_value = cast(
            list[Any] | tuple[Any, ...],
            value,
        )
        for index, item in enumerate(sequence_value):
            _check_json_value(
                item,
                f"{path}[{index}]",
            )


class CreatePaymentRequest(BaseModel):
    """Проверенная схема тела запроса на создание платежа."""

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

    @field_validator("description")
    @classmethod
    def _description_chars(
        cls,
        value: str,
    ) -> str:
        return _reject_nul(
            value,
            "description",
        )

    @field_validator("metadata")
    @classmethod
    def _metadata_size(
        cls,
        value: dict[str, Any],
    ) -> dict[str, Any]:
        _check_json_value(
            value,
            "metadata",
        )
        # allow_nan=False: невалидный JSON не должен попасть в БД/событие
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must be at most {MAX_METADATA_BYTES} bytes")
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
