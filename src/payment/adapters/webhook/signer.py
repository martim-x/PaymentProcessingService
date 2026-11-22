"""HMAC-подпись webhook-уведомлений.

Подпись считается от ТЕХ ЖЕ байтов, которые уходят в сеть:
`serialize` — единственный сериализатор для отправки и проверки.
Формат заголовка X-Signature: ``t=<unix-ts>,v1=<hex>``; HMAC берётся от
``f"{timestamp}.".encode() + body`` (timestamp защищает от replay-атак).
"""

import hashlib
import hmac
import json
import time
from typing import Any

DEFAULT_TOLERANCE_SECONDS = 300


def serialize(payload: dict[str, Any]) -> bytes:
    """Канонический JSON-байтстрим: стабильный порядок ключей, без пробелов."""
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
    ).encode("utf-8")


def _digest(
    secret: str,
    body: bytes,
    timestamp: int,
) -> str:
    message = str(timestamp).encode("ascii") + b"." + body
    return hmac.new(
        secret.encode("utf-8"),
        message,
        hashlib.sha256,
    ).hexdigest()


def sign_body(
    secret: str,
    body: bytes,
    timestamp: int | None = None,
) -> tuple[str, int]:
    """Возвращает (значение X-Signature, timestamp) для готового тела запроса."""
    ts = int(timestamp if timestamp is not None else time.time())
    return f"t={ts},v1={_digest(secret, body, ts,)}", ts


def parse_signature_header(header: str) -> tuple[int, str] | None:
    parts = dict(
        chunk.split(
            "=",
            1,
        )
        for chunk in header.split(",")
        if "=" in chunk
    )
    try:
        return int(parts["t"]), parts["v1"]
    except (
        KeyError,
        ValueError,
    ):
        return None


def verify_body(
    secret: str,
    body: bytes,
    header: str,
    *,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
    now: int | None = None,
) -> bool:
    """Проверка подписи получателем: те же байты + допустимое окно по времени."""
    parsed = parse_signature_header(header)
    if parsed is None:
        return False
    timestamp, provided = parsed
    current = now if now is not None else int(time.time())
    if abs(current - timestamp) > tolerance:
        return False
    expected = _digest(
        secret,
        body,
        timestamp,
    )
    return hmac.compare_digest(
        expected,
        provided,
    )
