"""Перепубликация retry/DLQ: сбой publish не должен терять исходное сообщение."""

import pytest
from pamqp.commands import Basic

from payment.adapters.messaging.publisher import FastStreamPublisher
from payment.adapters.messaging.redelivery import MessageRedeliverer
from payment.adapters.messaging.routing import BrokerRetryHeader, FailureRoute
from payment.application.ports.message_publisher import (
    PublisherUnavailable,
    PublishRejected,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


def _is_unavailable(exc: BaseException) -> bool:
    return FastStreamPublisher._is_unavailable(exc)


def ensure_confirmed(confirmation: object, subject: str) -> None:
    FastStreamPublisher.ensure_confirmed(confirmation=confirmation, subject=subject)


async def publish_failure(broker, message, payload, decision):
    return await MessageRedeliverer.publish_failure(
        broker=broker, message=message, payload=payload, decision=decision
    )


async def publish_or_requeue(**kwargs):
    return await MessageRedeliverer.publish_or_requeue(**kwargs)


class FakeMessage:
    def __init__(
        self,
        *,
        headers: dict | None = None,
    ) -> None:
        self.acked = False
        self.requeued = False
        self.headers = headers or {}

    async def ack(self) -> None:
        self.acked = True

    async def nack(
        self,
        *,
        requeue: bool = True,
    ) -> None:
        self.requeued = requeue


class FakeBroker:
    def __init__(
        self,
        *,
        fail: bool = False,
        confirmation: object = True,
    ) -> None:
        self.fail = fail
        self.confirmation = confirmation
        self.published: list[dict] = []

    async def publish(
        self,
        payload,
        **kwargs,
    ) -> object:
        if self.fail:
            raise RuntimeError("queue vanished")
        self.published.append(
            {
                "payload": payload,
                **kwargs,
            }
        )
        return self.confirmation


class TestPublishOrRequeue:
    @pytest.mark.parametrize(
        "confirmation",
        [
            None,
            False,
            Basic.Nack(),
        ],
    )
    async def test_unconfirmed_publish_requeues_original(
        self,
        confirmation: object,
    ) -> None:
        message = FakeMessage()
        ok = await publish_or_requeue(
            broker=FakeBroker(confirmation=confirmation),  # type: ignore[arg-type]
            message=message,  # type: ignore[arg-type]
            payload={"n": 1},
            queue="payments.new.retry.1",
        )

        assert ok is False
        assert not message.acked
        assert message.requeued

    async def test_success_publishes_then_acks(self) -> None:
        message = FakeMessage()
        broker = FakeBroker()

        ok = await publish_or_requeue(
            broker=broker,  # type: ignore[arg-type]
            message=message,  # type: ignore[arg-type]
            payload={"n": 1},
            queue="payments.new.retry.1",
        )

        assert ok is True
        assert message.acked is True
        assert message.requeued is False
        assert broker.published[0]["persist"] is True
        assert broker.published[0]["mandatory"] is True

    async def test_publish_failure_requeues_instead_of_losing(self) -> None:
        message = FakeMessage()
        broker = FakeBroker(fail=True)

        ok = await publish_or_requeue(
            broker=broker,  # type: ignore[arg-type]
            message=message,  # type: ignore[arg-type]
            payload={"n": 1},
            queue="payments.new.retry.1",
        )

        assert ok is False
        assert not message.acked
        assert message.requeued

    @pytest.mark.parametrize(
        "confirmation",
        [
            None,
            False,
            Basic.Nack(),
        ],
    )
    async def test_failure_publish_uses_confirms_before_ack(
        self,
        confirmation: object,
    ) -> None:
        message = FakeMessage()
        broker = FakeBroker(confirmation=confirmation)
        decision = FailureRoute(
            action="retry",
            attempt=1,
            queue="payments.new.retry.1",
        )

        ok = await publish_failure(
            broker,  # type: ignore[arg-type]
            message,  # type: ignore[arg-type]
            {"event": "payment.created"},
            decision,
        )

        assert ok is False
        assert not message.acked
        assert message.requeued

    async def test_unroutable_mandatory_publish_requeues_original(self) -> None:
        class UnroutableBroker(FakeBroker):
            async def publish(
                self,
                payload,
                **kwargs,
            ) -> object:
                raise RuntimeError("mandatory publish returned an unroutable message")

        message = FakeMessage()
        ok = await publish_or_requeue(
            broker=UnroutableBroker(),  # type: ignore[arg-type]
            message=message,  # type: ignore[arg-type]
            payload={"event": "payment.created"},
            queue="payments.new.retry.1",
        )

        assert ok is False
        assert not message.acked
        assert message.requeued

    async def test_retry_decision_carries_attempt_header(self) -> None:
        message = FakeMessage()
        broker = FakeBroker()
        decision = FailureRoute(
            action="retry",
            attempt=2,
            queue="payments.new.retry.2",
            header_value=2,
        )

        await publish_failure(
            broker,  # type: ignore[arg-type]
            message,  # type: ignore[arg-type]
            {"n": 1},
            decision,
        )

        headers = broker.published[0]["headers"]

        assert headers == {BrokerRetryHeader.ATTEMPT.value: 2}
        assert all(type(key) is str for key in headers)

    async def test_dlq_decision_has_no_attempt_header(self) -> None:
        message = FakeMessage()
        broker = FakeBroker()
        decision = FailureRoute(
            action="dlq",
            attempt=3,
            queue="payments.new.dlq",
        )

        await publish_failure(
            broker,  # type: ignore[arg-type]
            message,  # type: ignore[arg-type]
            {"n": 1},
            decision,
        )

        assert broker.published[0]["headers"] is None
        assert message.acked is True


class TestConfirmations:
    def test_nack_is_a_record_failure_not_an_outage(self) -> None:
        with pytest.raises(PublishRejected):
            ensure_confirmed(
                Basic.Nack(),
                "payments.new",
            )

    def test_bool_false_is_a_record_failure(self) -> None:
        with pytest.raises(PublishRejected):
            ensure_confirmed(
                False,
                "payments.new",
            )

    def test_missing_confirmation_is_an_outage(self) -> None:
        with pytest.raises(PublisherUnavailable):
            ensure_confirmed(
                None,
                "payments.new",
            )

    def test_ack_is_accepted(self) -> None:
        ensure_confirmed(
            Basic.Ack(),
            "payments.new",
        )

    def test_rejection_is_not_classified_as_unavailable(self) -> None:
        assert _is_unavailable(PublishRejected(message="nack")) is False
