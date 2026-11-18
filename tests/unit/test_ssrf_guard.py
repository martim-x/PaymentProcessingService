"""SSRF-гард: webhook_url не должен вести внутрь сети."""

import pytest

from payment.adapters.webhook.guard import UnsafeWebhookUrl, validate_public_http_url


class TestValidatePublicHttpUrl:
    def test_allows_public_ip(self) -> None:
        validate_public_http_url("https://93.184.216.34/hook")

    def test_rejects_loopback_ip(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url("http://127.0.0.1/hook")

    def test_rejects_private_ip(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url("http://192.168.0.10/hook")

    def test_rejects_link_local_metadata_address(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url("http://169.254.169.254/latest/meta-data")

    def test_rejects_localhost_name(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url("http://localhost:9000/hook")

    def test_rejects_non_http_scheme(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url("ftp://example.com/hook")

    def test_rejects_unresolvable_host(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            validate_public_http_url("http://no-such-host.invalid/hook")
