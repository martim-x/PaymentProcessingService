"""Prefetch на канале FastStream и параметры публикации (persist/mandatory/confirms)."""

import pytest

from payment.adapters.messaging.publisher import FastStreamPublisher
from payment.application.ports.message_publisher import PublisherUnavailable
from payment.bootstrap import make_broker, make_channel
from payment.config import Settings


def settings(**overrides) -> Settings:
    base = {
        "api_key": "test-api-key",
        "webhook_secret": "test-webhook-secret",
        "database_url": "postgresql+psycopg://u:p@localhost:5432/db",
        "rabbitmq_url": "amqp://guest:guest@localhost:5672/",
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


class TestPrefetch:
    def test_prefetch_is_set_on_the_channel(self) -> None:
        """consume_args подписчика не доходил до QoS: prefetch живёт на канале."""
        channel = make_channel(settings(consumer_prefetch=7))

        assert channel.prefetch_count == 7

    def test_channel_enables_confirms_and_mandatory_returns(self) -> None:
        channel = make_channel(settings())

        assert channel.publisher_confirms is True
        assert channel.on_return_raises is True

    def test_broker_uses_that_channel(self) -> None:
        broker = make_broker(settings(consumer_prefetch=3))

        channel = broker.config.channel_manager._ChannelManagerImpl__default_channel  # noqa: SLF001
        assert channel.prefetch_count == 3


class TestQueueConfig:
    def test_webhook_claim_must_cover_http_timeout(self) -> None:
        with pytest.raises(ValueError, match="WEBHOOK_CLAIM_TTL_SECONDS"):
            settings(webhook_timeout=10.0, webhook_claim_ttl_seconds=5.0)


class FakeBroker:
    def __init__(self, confirmation: object = object()) -> None:
        self.confirmation = confirmation
        self.calls: list[dict] = []

    async def publish(self, payload, **kwargs):
        self.calls.append({"payload": payload, **kwargs})
        return self.confirmation


class TestPublisher:
    async def test_publish_is_persistent_and_mandatory(self) -> None:
        broker = FakeBroker()

        await FastStreamPublisher(broker).publish(  # type: ignore[arg-type]
            subject="payments.new",
            payload={"event": "payment.webhook"},
            message_id="evt-1",
        )

        call = broker.calls[0]
        assert call["queue"] == "payments.new"
        assert call["persist"] is True
        assert call["mandatory"] is True
        assert call["message_id"] == "evt-1"

    async def test_missing_confirmation_is_treated_as_broker_outage(self) -> None:
        broker = FakeBroker(confirmation=None)

        with pytest.raises(PublisherUnavailable):
            await FastStreamPublisher(broker).publish(  # type: ignore[arg-type]
                subject="payments.new",
                payload={},
                message_id="evt-1",
            )

    async def test_unroutable_message_is_a_record_failure(self) -> None:
        from aio_pika.exceptions import DeliveryError

        class RejectingBroker(FakeBroker):
            async def publish(self, payload, **kwargs):
                raise DeliveryError("unroutable", None)  # type: ignore[arg-type]

        with pytest.raises(DeliveryError):
            await FastStreamPublisher(RejectingBroker()).publish(  # type: ignore[arg-type]
                subject="payments.new",
                payload={},
            )
