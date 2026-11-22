"""Канонический fingerprint тела запроса — для детекции конфликта idempotency key."""

import hashlib
import json
from typing import Any, cast

from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import Money


def _canonical(value: Any) -> Any:
    if isinstance(
        value,
        Money,
    ):
        return {
            "amount": str(value.amount),
            "currency": value.currency.value,
        }
    if isinstance(
        value,
        Currency,
    ):
        return value.value
    if isinstance(
        value,
        dict,
    ):
        typed_value = cast(
            dict[Any, Any],
            value,
        )
        return {
            str(key): _canonical(item)
            for key, item in sorted(
                typed_value.items(),
                key=lambda pair: str(pair[0]),
            )
        }
    if isinstance(
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
        return [_canonical(item) for item in sequence_value]
    return value


def compute_fingerprint(body: dict[str, Any]) -> str:
    """sha256 от канонического JSON тела; порядок ключей не влияет."""
    canonical = json.dumps(
        _canonical(body),
        ensure_ascii=False,
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
