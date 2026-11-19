"""Настройки: бюджет ожидания дубликата обязан покрывать TTL клейма."""

import pytest

from payment.config import Settings


def make_settings(**overrides) -> Settings:
    return Settings(api_key="k" * 10, webhook_secret="s" * 10, **overrides)


class TestClaimBudget:
    def test_defaults_satisfy_the_invariant(self) -> None:
        settings = make_settings()

        assert settings.claim_ttl_seconds + 2 * settings.claim_poll_seconds < (
            settings.retry_wait_budget
        )

    def test_ttl_below_gateway_time_is_rejected(self) -> None:
        # короче шлюза: второй consumer перехватил бы клейм у работающего первого
        with pytest.raises(ValueError, match="must be at least"):
            make_settings(claim_ttl_seconds=3, process_max_seconds=5.0)

    def test_ttl_must_cover_gateway_plus_finalize_margin(self) -> None:
        # 5с шлюза + 5с запаса на финализацию = 10с минимум
        with pytest.raises(ValueError, match="at least"):
            make_settings(claim_ttl_seconds=8, process_max_seconds=5.0)

    def test_ttl_needs_strict_margin_below_budget(self) -> None:
        # budget = 48 при дефолтах: 47 + 2*0.5 = 48 -> запаса нет, отклоняем
        with pytest.raises(ValueError, match="duplicate-wait budget"):
            make_settings(claim_ttl_seconds=47)

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

    def test_ttl_just_below_the_strict_limit_is_accepted(self) -> None:
        settings = make_settings(claim_ttl_seconds=46, claim_wait_seconds=15)

        assert settings.claim_ttl_seconds + 2 * settings.claim_poll_seconds < (
            settings.retry_wait_budget
        )

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

    @pytest.mark.parametrize(
        ("api_key", "webhook_secret", "message"),
        [
            ("change-this-api-key", "s" * 10, "PAYMENT_API_KEY"),
            ("k" * 10, "change-this-webhook-secret", "PAYMENT_WEBHOOK_SECRET"),
        ],
    )
    def test_env_example_placeholders_are_rejected(
        self, api_key: str, webhook_secret: str, message: str
    ) -> None:
        with pytest.raises(ValueError, match=message):
            Settings(api_key=api_key, webhook_secret=webhook_secret)
