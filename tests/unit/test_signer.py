"""HMAC-подпись webhook: подписываются ровно те байты, что уходят в сеть."""

from payment.adapters.webhook.signer import (
    parse_signature_header,
    serialize,
    sign_body,
    verify_body,
)

SECRET = "test-secret"
PAYLOAD = {"id": "abc", "status": "succeeded", "amount": "10.0000"}


class TestSerialize:
    def test_key_order_does_not_change_bytes(self) -> None:
        a = serialize({"a": 1, "b": 2})
        b = serialize({"b": 2, "a": 1})

        assert a == b

    def test_cyrillic_is_not_escaped(self) -> None:
        assert "Заказ".encode() in serialize({"description": "Заказ"})


class TestSignBody:
    def test_signature_header_format(self) -> None:
        header, ts = sign_body(SECRET, serialize(PAYLOAD))

        assert header.startswith(f"t={ts},v1=")
        assert parse_signature_header(header) is not None

    def test_verify_accepts_correct_signature(self) -> None:
        body = serialize(PAYLOAD)
        header, _ = sign_body(SECRET, body)

        assert verify_body(SECRET, body, header) is True

    def test_verify_rejects_tampered_body(self) -> None:
        body = serialize(PAYLOAD)
        header, _ = sign_body(SECRET, body)
        tampered = body.replace(b"succeeded", b"failed")

        assert verify_body(SECRET, tampered, header) is False

    def test_verify_rejects_wrong_secret(self) -> None:
        body = serialize(PAYLOAD)
        header, _ = sign_body(SECRET, body)

        assert verify_body("other-secret", body, header) is False

    def test_verify_rejects_stale_timestamp(self) -> None:
        body = serialize(PAYLOAD)
        header, ts = sign_body(SECRET, body, timestamp=1_000_000)

        assert verify_body(SECRET, body, header, now=ts + 10_000) is False

    def test_verify_rejects_malformed_header(self) -> None:
        assert verify_body(SECRET, serialize(PAYLOAD), "sha256=deadbeef") is False
