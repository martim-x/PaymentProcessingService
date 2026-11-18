"""Чистые функции consumer'а (тестируются отдельно от брокера)."""

import uuid

from payment.domain.model.value_objects import PaymentId

ATTEMPT_HEADER = "x-attempt"


def next_attempt(headers: dict) -> int:
    """Номер следующей попытки обработки.

    Основной источник — собственный заголовок `x-attempt`: x-death теряется
    при повторной публикации (новое сообщение), поэтому счётчик обязан ехать
    вместе с сообщением. `x-death` остаётся fallback'ом для сообщений,
    отправленных до апгрейда.
    """
    own = headers.get(ATTEMPT_HEADER)
    if own is not None:
        try:
            return int(own) + 1
        except (TypeError, ValueError):
            return 1
    attempts = 0
    for death in headers.get("x-death") or []:
        if str(death.get("queue", "")).startswith("payments.new.retry."):
            attempts += int(death.get("count", 1))
    return attempts + 1


def parse_payment_id(payload: dict) -> PaymentId:
    return PaymentId(value=uuid.UUID(str(payload["payment_id"])))
