"""Счётчик попыток consumer'а: x-attempt переживает повторную публикацию."""

from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy
from payment.entrypoints.consumer.handlers import ATTEMPT_HEADER, next_attempt


class TestNextAttempt:
    def test_first_delivery_is_attempt_one(self) -> None:
        assert next_attempt({}) == 1

    def test_own_header_increments(self) -> None:
        assert next_attempt({ATTEMPT_HEADER: 1}) == 2
        assert next_attempt({ATTEMPT_HEADER: 2}) == 3

    def test_own_header_has_priority_over_x_death(self) -> None:
        headers = {ATTEMPT_HEADER: 2, "x-death": [{"queue": "payments.new.retry.1", "count": 1}]}

        assert next_attempt(headers) == 3

    def test_x_death_fallback_still_works(self) -> None:
        headers = {"x-death": [{"queue": "payments.new.retry.2", "count": 1}]}

        assert next_attempt(headers) == 2

    def test_x_death_ignores_foreign_queues(self) -> None:
        headers = {"x-death": [{"queue": "some.other.queue", "count": 5}]}

        assert next_attempt(headers) == 1

    def test_broken_header_value_resets_to_first_attempt(self) -> None:
        assert next_attempt({ATTEMPT_HEADER: "not-a-number"}) == 1


class TestRetryChainReachesDlq:
    """Главная регрессия: цепочка попыток обязана дойти до DLQ, а не крутиться вечно."""

    def test_chain_ends_after_max_attempts(self) -> None:
        policy = ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=1.0, factor=2.0)
        headers: dict = {}
        attempts = []

        for _ in range(10):  # имитируем бесконечную доставку сбоящего сообщения
            attempt = next_attempt(headers)
            attempts.append(attempt)
            if policy.is_exhausted(attempts=attempt):
                break
            headers = {ATTEMPT_HEADER: attempt}

        assert attempts == [1, 2, 3]
