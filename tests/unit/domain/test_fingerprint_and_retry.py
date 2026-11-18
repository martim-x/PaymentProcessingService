"""Домен: fingerprint (канонический хэш тела) и retry policy."""

from decimal import Decimal

from payment.domain.model.enums import Currency
from payment.domain.model.fingerprint import compute_fingerprint
from payment.domain.model.value_objects import Money
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy


class TestFingerprint:
    def test_same_data_same_hash(self) -> None:
        body = {"amount": "100.00", "currency": "RUB", "description": "d"}

        assert compute_fingerprint(body) == compute_fingerprint(dict(body))

    def test_key_order_does_not_matter(self) -> None:
        a = compute_fingerprint({"amount": "100.00", "currency": "RUB"})
        b = compute_fingerprint({"currency": "RUB", "amount": "100.00"})

        assert a == b

    def test_different_data_different_hash(self) -> None:
        a = compute_fingerprint({"amount": "100.00"})
        b = compute_fingerprint({"amount": "200.00"})

        assert a != b

    def test_nested_metadata_stable(self) -> None:
        a = compute_fingerprint({"metadata": {"a": 1, "b": {"c": 2}}})
        b = compute_fingerprint({"metadata": {"b": {"c": 2}, "a": 1}})

        assert a == b


class TestExponentialBackoffRetryPolicy:
    def test_three_attempts_configured(self) -> None:
        policy = ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=1.0, factor=2.0)

        assert policy.max_attempts == 3
        assert policy.is_exhausted(attempts=3) is True
        assert policy.is_exhausted(attempts=2) is False

    def test_delay_grows_exponentially(self) -> None:
        policy = ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=1.0, factor=2.0)

        assert policy.delay_for_attempt(1) == 1.0
        assert policy.delay_for_attempt(2) == 2.0
        assert policy.delay_for_attempt(3) == 4.0

    def test_money_in_fingerprint_module_reimport_works(self) -> None:
        # защита от циклических импортов: value_objects используются в fingerprint
        m = Money(amount=Decimal("1"), currency=Currency.RUB)
        assert compute_fingerprint({"amount": str(m.amount)}) is not None
