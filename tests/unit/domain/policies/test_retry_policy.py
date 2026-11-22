from decimal import Decimal

import pytest

from payment.domain.model.enums import Currency
from payment.domain.model.fingerprint import PaymentFingerprint
from payment.domain.model.value_objects import Money
from payment.domain.policies.retry_policy import ExponentialBackoffRetryPolicy

pytestmark = [
    pytest.mark.unit,
    pytest.mark.domain,
]


class TestExponentialBackoffRetryPolicy:
    def test_three_attempts_configured(self) -> None:
        policy = ExponentialBackoffRetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            factor=2.0,
        )

        assert policy.max_attempts == 3
        assert policy.is_exhausted(attempts=3) is True
        assert policy.is_exhausted(attempts=2) is False

    def test_delay_grows_exponentially(self) -> None:
        policy = ExponentialBackoffRetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            factor=2.0,
        )

        assert policy.delay_for_attempt(attempt=1) == 1.0
        assert policy.delay_for_attempt(attempt=2) == 2.0
        assert policy.delay_for_attempt(attempt=3) == 4.0

    def test_money_in_fingerprint_module_reimport_works(self) -> None:
        # защита от циклических импортов: value_objects используются в fingerprint
        m = Money(
            amount=Decimal("1"),
            currency=Currency.RUB,
        )
        assert PaymentFingerprint.compute(payload={"amount": str(m.amount)}) is not None
