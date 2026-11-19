"""Настройки: бюджет ожидания дубликата обязан покрывать TTL клейма."""

import pytest

from payment.config import Settings


def make_settings(**overrides) -> Settings:
    return Settings(api_key="k" * 10, webhook_secret="s" * 10, **overrides)


class TestClaimBudget:
    def test_defaults_satisfy_the_invariant(self) -> None:
        settings = make_settings()

        assert settings.claim_ttl_seconds <= settings.retry_wait_budget

    def test_budget_counts_waits_and_retry_ttls(self) -> None:
        settings = make_settings(
            max_attempts=3,
            claim_wait_seconds=10,
            retry_base_delay=1.0,
            retry_factor=2.0,
        )

        # 3 ожидания по 10с + TTL retry.1 (1с) и retry.2 (2с)
        assert settings.retry_wait_budget == pytest.approx(33.0)

    def test_ttl_above_budget_is_rejected(self) -> None:
        # сценарий из ревью: TTL 60с не даёт дубликату дожить до перехвата
        with pytest.raises(ValueError, match="duplicate-wait budget"):
            make_settings(claim_ttl_seconds=60, claim_wait_seconds=15)

    def test_ttl_can_be_reduced_to_fit(self) -> None:
        settings = make_settings(claim_ttl_seconds=30, claim_wait_seconds=15)

        assert settings.claim_ttl_seconds < settings.retry_wait_budget

    def test_bigger_wait_allows_bigger_ttl(self) -> None:
        settings = make_settings(claim_ttl_seconds=60, claim_wait_seconds=25)

        assert settings.claim_ttl_seconds <= settings.retry_wait_budget


class TestSecrets:
    def test_empty_key_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="PAYMENT_API_KEY"):
            Settings(api_key="", webhook_secret="s" * 10)

    def test_default_placeholder_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="PAYMENT_WEBHOOK_SECRET"):
            Settings(api_key="k" * 10, webhook_secret="change-me-webhook-signing-secret")
