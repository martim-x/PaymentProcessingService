"""HMAC-подпись webhook-уведомлений."""

import hashlib
import hmac
import json
from typing import Any


def sign_payload(secret: str, payload: dict[str, Any]) -> str:
    """Возвращает 'sha256=<hex>' HMAC-SHA256 от канонического JSON тела."""
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_payload(secret: str, payload: dict[str, Any], signature: str) -> bool:
    expected = sign_payload(secret, payload)
    return hmac.compare_digest(expected, signature)
