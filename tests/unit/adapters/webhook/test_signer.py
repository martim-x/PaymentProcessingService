"""HMAC-подпись webhook: подписываются ровно те байты, что уходят в сеть."""

import pytest

from payment.adapters.webhook.signer import WebhookSigner

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


SECRET = "test-secret"
PAYLOAD = {
    "id": "abc",
    "status": "succeeded",
    "amount": "10.0000",
}


class TestSerialize:
    def test_key_order_does_not_change_bytes(self) -> None:
        a = WebhookSigner.serialize(
            {
                "a": 1,
                "b": 2,
            }
        )
        b = WebhookSigner.serialize(
            {
                "b": 2,
                "a": 1,
            }
        )

        assert a == b

    def test_cyrillic_is_not_escaped(self) -> None:
        assert "Заказ".encode() in WebhookSigner.serialize({"description": "Заказ"})


class TestSignBody:
    def test_known_hmac_vector_and_signature_header_format(self) -> None:
        body = b"{}"
        header, ts = WebhookSigner.sign_body(
            "secret",
            body,
            timestamp=1,
        )

        assert (
            header
            == "t=1,v1=1122767b193110cfec322b6f199b599edbf608ed087f2d27afb0b97d99523908"
        )
        assert WebhookSigner.parse_signature_header(header) == (
            ts,
            header.split("v1=", 1)[1],
        )
        assert WebhookSigner.parse_signature_header(header) is not None

    def test_verify_accepts_correct_signature(self) -> None:
        body = WebhookSigner.serialize(PAYLOAD)
        header, _ = WebhookSigner.sign_body(
            SECRET,
            body,
        )

        assert (
            WebhookSigner.verify_body(
                SECRET,
                body,
                header,
            )
            is True
        )

    def test_verify_rejects_tampered_body(self) -> None:
        body = WebhookSigner.serialize(PAYLOAD)
        header, _ = WebhookSigner.sign_body(
            SECRET,
            body,
        )
        tampered = body.replace(
            b"succeeded",
            b"failed",
        )

        assert (
            WebhookSigner.verify_body(
                SECRET,
                tampered,
                header,
            )
            is False
        )

    def test_verify_rejects_wrong_secret(self) -> None:
        body = WebhookSigner.serialize(PAYLOAD)
        header, _ = WebhookSigner.sign_body(
            SECRET,
            body,
        )

        assert (
            WebhookSigner.verify_body(
                "other-secret",
                body,
                header,
            )
            is False
        )

    def test_verify_rejects_stale_timestamp(self) -> None:
        body = WebhookSigner.serialize(PAYLOAD)
        header, ts = WebhookSigner.sign_body(
            SECRET,
            body,
            timestamp=1_000_000,
        )

        assert (
            WebhookSigner.verify_body(
                SECRET,
                body,
                header,
                now=ts + 10_000,
            )
            is False
        )

    @pytest.mark.parametrize(
        ("offset", "accepted"),
        [(-301, False), (-300, True), (300, True), (301, False)],
    )
    def test_default_tolerance_includes_both_boundaries(
        self, offset: int, accepted: bool
    ) -> None:
        body = b"{}"
        header, ts = WebhookSigner.sign_body(SECRET, body, timestamp=1_000_000)

        assert (
            WebhookSigner.verify_body(SECRET, body, header, now=ts + offset) is accepted
        )

    def test_verify_rejects_malformed_header(self) -> None:
        assert (
            WebhookSigner.verify_body(
                SECRET,
                WebhookSigner.serialize(PAYLOAD),
                "sha256=deadbeef",
            )
            is False
        )
