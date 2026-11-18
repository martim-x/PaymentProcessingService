"""Политика маршрутизации упавших сообщений (retry-очередь или DLQ).

Живёт в адаптере, а не в обработчике: entrypoint не должен решать,
сколько попыток и куда класть сообщение.
"""

from dataclasses import dataclass
from typing import Literal

from payment.adapters.messaging.topology import retry_queue_name
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

ATTEMPT_HEADER = "x-attempt"


@dataclass(frozen=True, slots=True)
class FailureRoute:
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


def route_failure(
    headers: dict,
    policy: ExponentialBackoffRetryPolicy,
    *,
    queue_name: str,
    dlq_name: str,
) -> FailureRoute:
    """Ретрай или DLQ: после policy.max_attempts сообщение уходит в DLQ."""
    attempt = next_attempt(headers)
    if policy.is_exhausted(attempts=attempt):
        return FailureRoute(action="dlq", attempt=attempt, queue=dlq_name)
    return FailureRoute(
        action="retry", attempt=attempt, queue=retry_queue_name(queue_name, attempt)
    )
