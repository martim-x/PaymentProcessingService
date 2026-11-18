"""Домен: Money, IdempotencyKey, WebhookUrl, PaymentId."""

from decimal import Decimal

import pytest

from payment.domain.exceptions import InvalidMoney, InvalidWebhookUrl
from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import (
    IdempotencyKey,
    Money,
    PaymentId,
    WebhookUrl,
)


class TestMoney:
    def test_creates_money_with_positive_amount(self) -> None:
        money = Money(amount=Decimal("100.50"), currency=Currency.RUB)

        assert money.amount == Decimal("100.50")
        assert money.currency == Currency.RUB

    def test_rejects_zero_amount(self) -> None:
        with pytest.raises(InvalidMoney):
            Money(amount=Decimal("0"), currency=Currency.USD)

    def test_rejects_negative_amount(self) -> None:
        with pytest.raises(InvalidMoney):
            Money(amount=Decimal("-10.00"), currency=Currency.USD)

    def test_normalizes_amount_to_four_digits(self) -> None:
        money = Money(amount=Decimal("10.123456"), currency=Currency.EUR)

        assert money.amount == Decimal("10.1235")


class TestIdempotencyKey:
    def test_accepts_non_empty_string(self) -> None:
        key = IdempotencyKey("order-123")

        assert key.value == "order-123"

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError):
            IdempotencyKey("")

    def test_rejects_whitespace_only(self) -> None:
        with pytest.raises(ValueError):
            IdempotencyKey("   ")

    def test_rejects_too_long(self) -> None:
        with pytest.raises(ValueError):
            IdempotencyKey("x" * 256)


class TestWebhookUrl:
    def test_accepts_https_url(self) -> None:
        url = WebhookUrl("https://example.com/hook")

        assert url.value == "https://example.com/hook"

    def test_accepts_http_url(self) -> None:
        WebhookUrl("http://example.com/hook")

    def test_rejects_empty(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl("")

    def test_rejects_no_scheme(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl("example.com/hook")

    def test_rejects_ftp_scheme(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl("ftp://example.com/hook")


class TestPaymentId:
    def test_generates_unique_ids(self) -> None:
        assert PaymentId.next() != PaymentId.next()

    def test_from_string_roundtrip(self) -> None:
        pid = PaymentId.next()

        assert PaymentId.from_string(str(pid)) == pid
