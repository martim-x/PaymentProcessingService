"""Домен: fingerprint (канонический хэш тела)."""

import pytest

from payment.domain.model.fingerprint import compute_fingerprint

pytestmark = [
    pytest.mark.unit,
    pytest.mark.domain,
]


class TestFingerprint:
    def test_same_data_same_hash(self) -> None:
        body = {
            "amount": "100.00",
            "currency": "RUB",
            "description": "d",
        }

        assert compute_fingerprint(body) == compute_fingerprint(dict(body))

    def test_key_order_does_not_matter(self) -> None:
        a = compute_fingerprint(
            {
                "amount": "100.00",
                "currency": "RUB",
            }
        )
        b = compute_fingerprint(
            {
                "currency": "RUB",
                "amount": "100.00",
            }
        )

        assert a == b

    def test_different_data_different_hash(self) -> None:
        a = compute_fingerprint({"amount": "100.00"})
        b = compute_fingerprint({"amount": "200.00"})

        assert a != b

    def test_nested_metadata_stable(self) -> None:
        a = compute_fingerprint(
            {
                "metadata": {
                    "a": 1,
                    "b": {"c": 2},
                }
            }
        )
        b = compute_fingerprint(
            {
                "metadata": {
                    "b": {"c": 2},
                    "a": 1,
                }
            }
        )

        assert a == b
