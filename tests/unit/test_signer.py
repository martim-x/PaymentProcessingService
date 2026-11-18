"""HMAC-подпись webhook: знак ставит сервис, проверяет получатель."""

from payment.adapters.webhook.signer import sign_payload, verify_payload

SECRET = "test-secret"
PAYLOAD = {"id": "abc", "status": "succeeded", "amount": "10.0000"}


class TestSigner:
    def test_signature_format(self) -> None:
        assert sign_payload(SECRET, PAYLOAD).startswith("sha256=")

    def test_verify_accepts_correct_signature(self) -> None:
        signature = sign_payload(SECRET, PAYLOAD)

        assert verify_payload(SECRET, PAYLOAD, signature) is True

    def test_verify_rejects_tampered_payload(self) -> None:
        signature = sign_payload(SECRET, PAYLOAD)
        tampered = {**PAYLOAD, "status": "failed"}

        assert verify_payload(SECRET, tampered, signature) is False

    def test_verify_rejects_wrong_secret(self) -> None:
        signature = sign_payload(SECRET, PAYLOAD)

        assert verify_payload("other-secret", PAYLOAD, signature) is False

    def test_signature_stable_regardless_of_key_order(self) -> None:
        a = sign_payload(SECRET, {"a": 1, "b": 2})
        b = sign_payload(SECRET, {"b": 2, "a": 1})

        assert a == b
