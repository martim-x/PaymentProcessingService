"""Политика маршрутизации упавших сообщений (retry-очередь или DLQ).

Живёт в адаптере, а не в обработчике: entrypoint не должен решать,
сколько попыток и куда класть сообщение.

Три сценария отказа:
- `FailureRouter.route_failure` — обычный отказ обработки;
- `FailureRouter.route_infra_failure` — транзиентный сбой с отдельной лестницей
  задержек;
- `FailureRouter.route_busy` — проигранная гонка за клейм, без расхода бюджета
  ошибок.

Бюджет попыток общий: предел `max_attempts` считается по СУММЕ business- и
инфраструктурных отказов, поэтому при любом их чередовании сообщение уходит в
DLQ ровно после третьей неудачи. Заголовки `x-attempt` и `x-infra-attempt`
остаются, но отвечают только за ступень TTL в своей лестнице задержек.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, cast

from payment.adapters.messaging.topology import RabbitTopology
from payment.domain.policies.retry_policy import ExponentialBackoffRetryPolicy


class BrokerRetryHeader(StrEnum):
    """Wire-имена служебных заголовков брокера.

    Значения — часть контракта с RabbitMQ и внешними потребителями DLQ:
    `x-attempt`, `x-infra-attempt`, `x-death`.
    """

    ATTEMPT = "x-attempt"
    INFRA_ATTEMPT = "x-infra-attempt"
    DEATH = "x-death"


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class FailureRoute:
    """Решение по упавшему сообщению.

    `attempt` — сколько отказов израсходовано всего (business + infra): по нему
    проверяется общий предел и пишется лог. `header_value` — значение своего
    счётчика класса отказа, из него строится номер retry-очереди.
    """

    action: Literal["retry", "dlq"]
    attempt: int
    queue: str
    header: BrokerRetryHeader = BrokerRetryHeader.ATTEMPT
    header_value: int = 1


class FailureRouter:
    """Чистая политика retry/DLQ: счётчики заголовков и выбор очереди."""

    @staticmethod
    def _own_attempt(
        headers: Mapping[str, Any],
        header: BrokerRetryHeader,
    ) -> int | None:
        """Значение своего счётчика; 0 — заголовок битый, None — заголовка нет."""
        own = headers.get(header)
        if own is None:
            return None
        try:
            return max(
                int(own),
                0,
            )
        except TypeError, ValueError:
            return 0

    @staticmethod
    def _death_records(headers: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        """Записи `x-death` как mapping'и.

        Содержимое заголовка приходит от брокера и не типизировано, поэтому всё,
        что не является списком словарей (битый или чужой формат), отбрасывается —
        так же, как раньше при обходе неожиданного значения.
        """
        raw = headers.get(BrokerRetryHeader.DEATH)
        if not isinstance(
            raw,
            (
                list,
                tuple,
            ),
        ):
            return []
        records: list[Mapping[str, Any]] = []
        for item in cast(
            "list[object] | tuple[object, ...]",
            raw,
        ):
            if isinstance(
                item,
                Mapping,
            ):
                records.append(
                    cast(
                        "Mapping[str, Any]",
                        item,
                    )
                )
        return records

    @staticmethod
    def current_attempt(
        headers: Mapping[str, Any],
        header: BrokerRetryHeader = BrokerRetryHeader.ATTEMPT,
        *,
        queue_name: str = RabbitTopology.MAIN_QUEUE,
    ) -> int:
        """Число учтённых попыток своего класса; x-death — fallback для business."""
        own = FailureRouter._own_attempt(
            headers,
            header,
        )
        if own is not None:
            return own
        if header != BrokerRetryHeader.ATTEMPT:
            return 0
        deaths = 0
        for death in FailureRouter._death_records(headers):
            queue = str(
                death.get(
                    "queue",
                    "",
                )
            )
            if (
                RabbitTopology.retry_attempt_from_queue(
                    queue_name,
                    queue,
                )
                is not None
            ):
                try:
                    deaths += max(
                        int(
                            death.get(
                                "count",
                                1,
                            )
                        ),
                        0,
                    )
                except TypeError, ValueError:
                    continue
        return deaths

    @staticmethod
    def total_attempts(
        headers: Mapping[str, Any],
        *,
        queue_name: str = RabbitTopology.MAIN_QUEUE,
    ) -> int:
        """Израсходовано отказов всего: business-счётчик плюс инфраструктурный."""
        return FailureRouter.current_attempt(
            headers,
            BrokerRetryHeader.ATTEMPT,
            queue_name=queue_name,
        ) + FailureRouter.current_attempt(
            headers,
            BrokerRetryHeader.INFRA_ATTEMPT,
            queue_name=queue_name,
        )

    @staticmethod
    def next_attempt(
        headers: Mapping[str, Any],
        header: BrokerRetryHeader = BrokerRetryHeader.ATTEMPT,
        *,
        queue_name: str = RabbitTopology.MAIN_QUEUE,
    ) -> int:
        """Номер следующей попытки обработки (своего класса)."""
        return (
            FailureRouter.current_attempt(
                headers,
                header,
                queue_name=queue_name,
            )
            + 1
        )

    @staticmethod
    def route_failure(
        headers: Mapping[str, Any],
        policy: ExponentialBackoffRetryPolicy,
        *,
        queue_name: str,
        dlq_name: str,
    ) -> FailureRoute:
        """Инкрементирует общий бюджет и отправляет исчерпавшее его сообщение в DLQ."""
        own = FailureRouter.next_attempt(
            headers,
            BrokerRetryHeader.ATTEMPT,
            queue_name=queue_name,
        )
        attempt = (
            FailureRouter.total_attempts(
                headers,
                queue_name=queue_name,
            )
            + 1
        )
        if policy.is_exhausted(attempts=attempt):
            return FailureRoute(
                action="dlq",
                attempt=attempt,
                queue=dlq_name,
                header_value=own,
            )
        return FailureRoute(
            action="retry",
            attempt=attempt,
            queue=RabbitTopology.retry_queue_name(
                queue_name,
                own,
            ),
            header_value=own,
        )

    @staticmethod
    def route_infra_failure(
        headers: Mapping[str, Any],
        policy: ExponentialBackoffRetryPolicy,
        *,
        queue_name: str,
        dlq_name: str,
    ) -> FailureRoute:
        """Тот же общий лимит; отдельный счётчик задаёт только ступень задержки."""
        own = FailureRouter.next_attempt(
            headers,
            BrokerRetryHeader.INFRA_ATTEMPT,
            queue_name=queue_name,
        )
        attempt = (
            FailureRouter.total_attempts(
                headers,
                queue_name=queue_name,
            )
            + 1
        )
        if policy.is_exhausted(attempts=attempt):
            return FailureRoute(
                action="dlq",
                attempt=attempt,
                queue=dlq_name,
                header=BrokerRetryHeader.INFRA_ATTEMPT,
                header_value=own,
            )
        return FailureRoute(
            action="retry",
            attempt=attempt,
            queue=RabbitTopology.infra_retry_queue_name(
                queue_name,
                own,
            ),
            header=BrokerRetryHeader.INFRA_ATTEMPT,
            header_value=own,
        )

    @staticmethod
    def route_busy(
        headers: Mapping[str, Any],
        policy: ExponentialBackoffRetryPolicy,
        *,
        queue_name: str,
    ) -> FailureRoute:
        """Отложить занятое сообщение без расхода общего бюджета ошибок."""
        attempt = FailureRouter.current_attempt(
            headers,
            queue_name=queue_name,
        )
        queue_attempt = max(
            1,
            min(
                attempt,
                policy.max_attempts - 1,
            ),
        )
        return FailureRoute(
            action="retry",
            attempt=attempt,
            queue=RabbitTopology.retry_queue_name(
                queue_name,
                queue_attempt,
            ),
            header_value=attempt,
        )
