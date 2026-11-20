"""Политика маршрутизации упавших сообщений (retry-очередь или DLQ).

Живёт в адаптере, а не в обработчике: entrypoint не должен решать,
сколько попыток и куда класть сообщение.

Три сценария отказа:
- `route_failure` — обычный отказ обработки;
- `route_infra_failure` — транзиентный сбой с отдельной лестницей задержек;
- `route_busy` — проигранная гонка за клейм, без расхода бюджета ошибок.

Бюджет попыток общий: предел `max_attempts` считается по СУММЕ business- и
инфраструктурных отказов, поэтому при любом их чередовании сообщение уходит в
DLQ ровно после третьей неудачи. Заголовки `x-attempt` и `x-infra-attempt`
остаются, но отвечают только за ступень TTL в своей лестнице задержек.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from payment.adapters.messaging.topology import (
    MAIN_QUEUE,
    infra_retry_queue_name,
    retry_attempt_from_queue,
    retry_queue_name,
)
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

ATTEMPT_HEADER = "x-attempt"
INFRA_ATTEMPT_HEADER = "x-infra-attempt"


@dataclass(frozen=True, slots=True)
class FailureRoute:
    """Решение по упавшему сообщению.

    `attempt` — сколько отказов израсходовано всего (business + infra): по нему
    проверяется общий предел и пишется лог. `header_value` — значение своего
    счётчика класса отказа, из него строится номер retry-очереди.
    """

    action: Literal["retry", "dlq"]
    attempt: int
    queue: str
    header: str = ATTEMPT_HEADER
    header_value: int = 1


def _own_attempt(headers: Mapping[str, Any], header: str) -> int | None:
    """Значение своего счётчика; 0 — заголовок битый, None — заголовка нет."""
    own = headers.get(header)
    if own is None:
        return None
    try:
        return max(int(own), 0)
    except (TypeError, ValueError):
        return 0


def current_attempt(
    headers: Mapping[str, Any], header: str = ATTEMPT_HEADER, *, queue_name: str = MAIN_QUEUE
) -> int:
    """Число учтённых попыток своего класса; x-death — fallback для business."""
    own = _own_attempt(headers, header)
    if own is not None:
        return own
    if header != ATTEMPT_HEADER:
        return 0
    deaths = 0
    for death in headers.get("x-death") or []:
        if not isinstance(death, Mapping):
            continue
        queue = str(death.get("queue", ""))
        if retry_attempt_from_queue(queue_name, queue) is not None:
            try:
                deaths += max(int(death.get("count", 1)), 0)
            except (TypeError, ValueError):
                continue
    return deaths


def total_attempts(headers: Mapping[str, Any], *, queue_name: str = MAIN_QUEUE) -> int:
    """Израсходовано отказов всего: business-счётчик плюс инфраструктурный."""
    return current_attempt(headers, ATTEMPT_HEADER, queue_name=queue_name) + current_attempt(
        headers, INFRA_ATTEMPT_HEADER, queue_name=queue_name
    )


def next_attempt(
    headers: Mapping[str, Any], header: str = ATTEMPT_HEADER, *, queue_name: str = MAIN_QUEUE
) -> int:
    """Номер следующей попытки обработки (своего класса)."""
    return current_attempt(headers, header, queue_name=queue_name) + 1


def route_failure(
    headers: Mapping[str, Any],
    policy: ExponentialBackoffRetryPolicy,
    *,
    queue_name: str,
    dlq_name: str,
) -> FailureRoute:
    """Инкрементирует общий бюджет и отправляет исчерпавшее его сообщение в DLQ."""
    own = next_attempt(headers, ATTEMPT_HEADER, queue_name=queue_name)
    attempt = total_attempts(headers, queue_name=queue_name) + 1
    if policy.is_exhausted(attempts=attempt):
        return FailureRoute(action="dlq", attempt=attempt, queue=dlq_name, header_value=own)
    return FailureRoute(
        action="retry",
        attempt=attempt,
        queue=retry_queue_name(queue_name, own),
        header_value=own,
    )


def route_infra_failure(
    headers: Mapping[str, Any],
    policy: ExponentialBackoffRetryPolicy,
    *,
    queue_name: str,
    dlq_name: str,
) -> FailureRoute:
    """Тот же общий лимит; отдельный счётчик задаёт только ступень задержки."""
    own = next_attempt(headers, INFRA_ATTEMPT_HEADER, queue_name=queue_name)
    attempt = total_attempts(headers, queue_name=queue_name) + 1
    if policy.is_exhausted(attempts=attempt):
        return FailureRoute(
            action="dlq",
            attempt=attempt,
            queue=dlq_name,
            header=INFRA_ATTEMPT_HEADER,
            header_value=own,
        )
    return FailureRoute(
        action="retry",
        attempt=attempt,
        queue=infra_retry_queue_name(queue_name, own),
        header=INFRA_ATTEMPT_HEADER,
        header_value=own,
    )


def route_busy(
    headers: Mapping[str, Any],
    policy: ExponentialBackoffRetryPolicy,
    *,
    queue_name: str,
) -> FailureRoute:
    """Отложить занятое сообщение без расхода общего бюджета ошибок."""
    attempt = current_attempt(headers, queue_name=queue_name)
    queue_attempt = max(1, min(attempt, policy.max_attempts - 1))
    return FailureRoute(
        action="retry",
        attempt=attempt,
        queue=retry_queue_name(queue_name, queue_attempt),
        header_value=attempt,
    )
