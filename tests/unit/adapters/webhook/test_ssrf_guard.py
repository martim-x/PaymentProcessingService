"""SSRF-гард: webhook_url не должен вести внутрь сети."""

import pytest

from payment.adapters.webhook.guard import UnsafeWebhookUrl, WebhookUrlGuard

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


class TestWebhookUrlGuard:
    def test_allows_public_ip(self) -> None:
        WebhookUrlGuard.validate("https://93.184.216.34/hook")

    def test_rejects_loopback_ip(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            WebhookUrlGuard.validate("http://127.0.0.1/hook")

    def test_rejects_private_ip(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            WebhookUrlGuard.validate("http://192.168.0.10/hook")

    def test_rejects_link_local_metadata_address(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            WebhookUrlGuard.validate("http://169.254.169.254/latest/meta-data")

    def test_rejects_localhost_name(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            WebhookUrlGuard.validate("http://localhost:9000/hook")

    def test_rejects_non_http_scheme(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            WebhookUrlGuard.validate("ftp://example.com/hook")

    def test_rejects_unresolvable_host(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            WebhookUrlGuard.validate("http://no-such-host.invalid/hook")

    async def test_async_wrapper_rejects_private_ip(self) -> None:
        with pytest.raises(UnsafeWebhookUrl):
            await WebhookUrlGuard.validate_async("http://127.0.0.1/hook")
