"""Домен: fingerprint (канонический хэш тела)."""

from decimal import Decimal

import pytest

from payment.domain.model.fingerprint import PaymentFingerprint

pytestmark = [
    pytest.mark.unit,
    pytest.mark.domain,
]


class TestFingerprint:
    def test_canonical_amount_preserves_precision_before_money_quantization(
        self,
    ) -> None:
        assert PaymentFingerprint.canonical_amount(amount=Decimal("1.0000100")) == (
            "1.00001"
        )
        assert PaymentFingerprint.canonical_amount(amount=Decimal("1E+2")) == "100"

    def test_same_data_same_hash(self) -> None:
        body = {
            "amount": "100.00",
            "currency": "RUB",
            "description": "d",
        }

        assert PaymentFingerprint.compute(payload=body) == PaymentFingerprint.compute(
            payload=dict(body)
        )

    def test_key_order_does_not_matter(self) -> None:
        a = PaymentFingerprint.compute(
            payload={
                "amount": "100.00",
                "currency": "RUB",
            },
        )
        b = PaymentFingerprint.compute(
            payload={
                "currency": "RUB",
                "amount": "100.00",
            },
        )

        assert a == b

    def test_different_data_different_hash(self) -> None:
        a = PaymentFingerprint.compute(payload={"amount": "100.00"})
        b = PaymentFingerprint.compute(payload={"amount": "200.00"})

        assert a != b

    def test_nested_metadata_stable(self) -> None:
        a = PaymentFingerprint.compute(
            payload={
                "metadata": {
                    "a": 1,
                    "b": {"c": 2},
                }
            },
        )
        b = PaymentFingerprint.compute(
            payload={
                "metadata": {
                    "b": {"c": 2},
                    "a": 1,
                }
            },
        )

        assert a == b
