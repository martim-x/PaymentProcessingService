"""Чистые функции consumer'а: решения о retry/DLQ (тестируются без брокера)."""

import uuid
from dataclasses import dataclass
from typing import Literal

from payment.domain.model.value_objects import PaymentId
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

ATTEMPT_HEADER = "x-attempt"


@dataclass(frozen=True, slots=True)
class FailureDecision:
    """Что делать с упавшим сообщением."""

    action: Literal["retry", "dlq"]
    attempt: int
    queue: str


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
        if ".retry." in str(death.get("queue", "")):
            attempts += int(death.get("count", 1))
    return attempts + 1


def decide_failure(
    headers: dict,
    policy: ExponentialBackoffRetryPolicy,
    *,
    queue_name: str,
    dlq_name: str,
) -> FailureDecision:
    """Ретрай или DLQ: после policy.max_attempts сообщение уходит в DLQ."""
    attempt = next_attempt(headers)
    if policy.is_exhausted(attempts=attempt):
        return FailureDecision(action="dlq", attempt=attempt, queue=dlq_name)
    return FailureDecision(action="retry", attempt=attempt, queue=f"{queue_name}.retry.{attempt}")


def parse_payment_id(payload: dict) -> PaymentId:
    return PaymentId(value=uuid.UUID(str(payload["payment_id"])))
