"""Канонический fingerprint тела запроса — для детекции конфликта idempotency key."""

import hashlib
import json
from typing import Any

from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import Money


def _canonical(value: Any) -> Any:
    if isinstance(value, Money):
        return {"amount": str(value.amount), "currency": value.currency.value}
    if isinstance(value, Currency):
        return value.value
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    return value


def compute_fingerprint(body: dict[str, Any]) -> str:
    """sha256 от канонического JSON тела; порядок ключей не влияет."""
    canonical = json.dumps(
        _canonical(body), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
