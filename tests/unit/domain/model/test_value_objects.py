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

pytestmark = [
    pytest.mark.unit,
    pytest.mark.domain,
]


class TestMoney:
    def test_from_raw_preserves_decimal_string_conversion(self) -> None:
        assert Money.from_raw(raw=0.1) == Decimal("0.1")

    def test_creates_money_with_positive_amount(self) -> None:
        money = Money(
            amount=Decimal("100.50"),
            currency=Currency.RUB,
        )

        assert money.amount == Decimal("100.50")
        assert money.currency == Currency.RUB

    def test_rejects_zero_amount(self) -> None:
        with pytest.raises(InvalidMoney):
            Money(
                amount=Decimal("0"),
                currency=Currency.USD,
            )

    def test_rejects_negative_amount(self) -> None:
        with pytest.raises(InvalidMoney):
            Money(
                amount=Decimal("-10.00"),
                currency=Currency.USD,
            )

    def test_rejects_positive_amount_that_rounds_to_zero(self) -> None:
        with pytest.raises(InvalidMoney):
            Money(
                amount=Decimal("0.00001"),
                currency=Currency.USD,
            )

    def test_rejects_amount_that_cannot_be_quantized(self) -> None:
        with pytest.raises(InvalidMoney):
            Money(
                amount=Decimal("1e1000"),
                currency=Currency.USD,
            )

    def test_normalizes_amount_to_four_digits(self) -> None:
        money = Money(
            amount=Decimal("10.123456"),
            currency=Currency.EUR,
        )

        assert money.amount == Decimal("10.1235")


class TestIdempotencyKey:
    def test_accepts_key_at_contract_length(self) -> None:
        value = "a" * 255
        assert IdempotencyKey(value=value).value == value

    def test_accepts_non_empty_string(self) -> None:
        key = IdempotencyKey(value="order-123")

        assert key.value == "order-123"

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError):
            IdempotencyKey(value="")

    def test_rejects_whitespace_only(self) -> None:
        with pytest.raises(ValueError):
            IdempotencyKey(value="   ")

    def test_rejects_too_long(self) -> None:
        with pytest.raises(ValueError):
            IdempotencyKey(value="x" * 256)


class TestWebhookUrl:
    def test_public_ip_predicate_is_owned_by_url(self) -> None:
        import ipaddress

        assert WebhookUrl._ip_is_public(ipaddress.ip_address("93.184.216.34"))
        assert not WebhookUrl._ip_is_public(ipaddress.ip_address("127.0.0.1"))

    def test_accepts_https_url(self) -> None:
        url = WebhookUrl(value="https://example.com/hook")

        assert url.value == "https://example.com/hook"

    def test_accepts_http_url(self) -> None:
        WebhookUrl(value="http://example.com/hook")

    def test_rejects_empty(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl(value="")

    def test_rejects_no_scheme(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl(value="example.com/hook")

    def test_rejects_ftp_scheme(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl(value="ftp://example.com/hook")

    def test_rejects_literal_private_ip(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl(value="http://10.0.0.5/hook")

    def test_rejects_literal_loopback_ip(self) -> None:
        with pytest.raises(InvalidWebhookUrl):
            WebhookUrl(value="http://127.0.0.1:9000/hook")

    def test_accepts_public_literal_ip(self) -> None:
        WebhookUrl(value="https://93.184.216.34/hook")


class TestPaymentId:
    def test_generates_unique_ids(self) -> None:
        assert PaymentId.next() != PaymentId.next()

    def test_from_string_roundtrip(self) -> None:
        pid = PaymentId.next()

        assert PaymentId.from_string(raw=str(pid)) == pid
