"""Счётчик попыток consumer'а: x-attempt переживает повторную публикацию."""

import pytest

from payment.adapters.messaging.routing import (
    BrokerRetryHeader,
    FailureRouter,
)
from payment.adapters.messaging.topology import RabbitTopology
from payment.domain.policies.retry_policy import ExponentialBackoffRetryPolicy

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


class TestNextAttempt:
    def test_first_delivery_is_attempt_one(self) -> None:
        assert FailureRouter.next_attempt({}) == 1

    def test_own_header_increments(self) -> None:
        assert FailureRouter.next_attempt({BrokerRetryHeader.ATTEMPT: 1}) == 2
        assert FailureRouter.next_attempt({BrokerRetryHeader.ATTEMPT: 2}) == 3

    def test_own_header_has_priority_over_x_death(self) -> None:
        headers = {
            BrokerRetryHeader.ATTEMPT: 2,
            "x-death": [
                {
                    "queue": "payments.new.retry.1",
                    "count": 1,
                }
            ],
        }

        assert FailureRouter.next_attempt(headers) == 3

    def test_x_death_fallback_still_works(self) -> None:
        headers = {
            "x-death": [
                {
                    "queue": "payments.new.retry.2",
                    "count": 1,
                }
            ]
        }

        assert FailureRouter.next_attempt(headers) == 2

    def test_x_death_fallback_ignores_infra_retry_queues(self) -> None:
        headers = {
            "x-death": [
                {
                    "queue": RabbitTopology.infra_retry_queue_name(
                        "payments.new",
                        1,
                    ),
                    "count": 4,
                },
                {
                    "queue": RabbitTopology.retry_queue_name(
                        "payments.new",
                        1,
                    ),
                    "count": 1,
                },
            ]
        }

        assert FailureRouter.next_attempt(headers) == 2

    def test_x_death_fallback_respects_custom_queue_names(self) -> None:
        queue_name = "payments.infra.retry.orders"
        headers = {
            "x-death": [
                {
                    "queue": RabbitTopology.infra_retry_queue_name(
                        queue_name,
                        1,
                    ),
                    "count": 4,
                },
                {
                    "queue": RabbitTopology.retry_queue_name(
                        queue_name,
                        1,
                    ),
                    "count": 1,
                },
            ]
        }

        assert (
            FailureRouter.next_attempt(
                headers,
                queue_name=queue_name,
            )
            == 2
        )
        decision = FailureRouter.route_failure(
            headers,
            ExponentialBackoffRetryPolicy(max_attempts=3),
            queue_name=queue_name,
            dlq_name="payments.dlq",
        )
        assert decision.attempt == 2
        assert decision.queue == RabbitTopology.retry_queue_name(
            queue_name,
            2,
        )

    def test_x_death_ignores_foreign_queues(self) -> None:
        headers = {
            "x-death": [
                {
                    "queue": "some.other.queue",
                    "count": 5,
                }
            ]
        }

        assert FailureRouter.next_attempt(headers) == 1

    def test_broken_header_value_resets_to_first_attempt(self) -> None:
        assert (
            FailureRouter.next_attempt({BrokerRetryHeader.ATTEMPT: "not-a-number"}) == 1
        )


class TestDecideFailure:
    def test_retry_targets_numbered_queue(self) -> None:
        policy = ExponentialBackoffRetryPolicy(max_attempts=3)

        decision = FailureRouter.route_failure(
            {BrokerRetryHeader.ATTEMPT: 1},
            policy,
            queue_name="payments.new",
            dlq_name="payments.dlq",
        )

        assert decision.action == "retry"
        assert decision.attempt == 2
        assert decision.queue == "payments.new.retry.2"

    def test_last_attempt_goes_to_dlq(self) -> None:
        policy = ExponentialBackoffRetryPolicy(max_attempts=3)

        decision = FailureRouter.route_failure(
            {BrokerRetryHeader.ATTEMPT: 2},
            policy,
            queue_name="payments.new",
            dlq_name="custom.dlq",
        )

        assert decision.action == "dlq"
        assert decision.attempt == 3
        assert decision.queue == "custom.dlq"

    def test_mixed_failures_reach_dlq_with_three_business_attempts(self) -> None:
        policy = ExponentialBackoffRetryPolicy(max_attempts=3)
        headers: dict[str, int] = {}
        decisions = []
        for _ in range(3):
            decision = FailureRouter.route_failure(
                headers,
                policy,
                queue_name="payments.new",
                dlq_name="payments.dlq",
            )
            decisions.append(decision)
            if decision.action == "retry":
                headers[BrokerRetryHeader.ATTEMPT] = decision.attempt
        assert [decision.action for decision in decisions] == [
            "retry",
            "retry",
            "dlq",
        ]
        assert decisions[-1].queue == "payments.dlq"


class TestRetryChainReachesDlq:
    """Главная регрессия: цепочка попыток обязана дойти до DLQ, а не крутиться вечно."""

    def test_chain_ends_after_max_attempts(self) -> None:
        policy = ExponentialBackoffRetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            factor=2.0,
        )
        headers: dict = {}
        attempts = []

        for _ in range(10):  # имитируем бесконечную доставку сбоящего сообщения
            attempt = FailureRouter.next_attempt(headers)
            attempts.append(attempt)
            if policy.is_exhausted(attempts=attempt):
                break
            headers = {BrokerRetryHeader.ATTEMPT: attempt}

        assert attempts == [
            1,
            2,
            3,
        ]

    def test_custom_queue_names_are_respected(self) -> None:
        policy = ExponentialBackoffRetryPolicy(max_attempts=2)

        decision = FailureRouter.route_failure(
            {},
            policy,
            queue_name="orders.new",
            dlq_name="orders.dlq",
        )

        assert decision.queue == "orders.new.retry.1"
