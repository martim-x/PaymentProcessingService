"""Relay: что публикуется, по какой очереди и с каким message_id."""

import pytest

from payment.adapters.messaging.topology import (
    DLQ_QUEUE,
    MAIN_QUEUE,
    main_queue,
    retry_queue,
    retry_queue_name,
)
from payment.application.ports.repositories import DISPATCH_LOCAL
from payment.application.services.outbox_service import (
    PAYMENT_CREATED_EVENT,
    OutboxService,
)
from payment.application.services.payment_service import WEBHOOK_EVENT
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy
from tests.fakes import FakeMessagePublisher, FakeUnitOfWork

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


def relay(
    uow: FakeUnitOfWork,
    publisher: FakeMessagePublisher,
) -> OutboxService:
    return OutboxService(
        uow=uow,
        publisher=publisher,
        routes={PAYMENT_CREATED_EVENT: MAIN_QUEUE},
        subject=MAIN_QUEUE,
    )


class TestEventRouting:
    async def test_created_event_goes_to_payment_queue(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(
            payload={
                "event": PAYMENT_CREATED_EVENT,
                "payment_id": "p1",
            },
            aggregate_id="p1",
        )
        publisher = FakeMessagePublisher()

        await relay(
            uow,
            publisher,
        ).publish_pending(batch=10)

        assert publisher.published[0][0] == MAIN_QUEUE

    async def test_local_webhook_record_is_never_published(self) -> None:
        """Запись для consumer'а не публикуется и не притворяется отправленной."""
        uow = FakeUnitOfWork()
        record = await uow.outbox.add(
            payload={
                "event": WEBHOOK_EVENT,
                "event_id": "evt-1",
                "payment_id": "p1",
                "webhook_url": "https://example.com/hook",
                "body": {"id": "p1"},
            },
            aggregate_id="p1",
            dispatch=DISPATCH_LOCAL,
        )
        publisher = FakeMessagePublisher()

        published = await relay(
            uow,
            publisher,
        ).publish_pending(batch=10)

        assert published == 0
        assert publisher.published == []
        assert record.sent is False
        assert record.dispatch == DISPATCH_LOCAL

    async def test_message_id_is_the_stable_event_id(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(
            payload={
                "event": PAYMENT_CREATED_EVENT,
                "event_id": "evt-stable",
            },
            aggregate_id="p1",
        )
        publisher = FakeMessagePublisher()

        await relay(
            uow,
            publisher,
        ).publish_pending(batch=10)

        assert publisher.message_ids == ["evt-stable"]

    async def test_unknown_event_falls_back_to_default_subject(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(
            payload={"event": "payment.unknown"},
            aggregate_id="p1",
        )
        publisher = FakeMessagePublisher()

        await relay(
            uow,
            publisher,
        ).publish_pending(batch=10)

        assert publisher.published[0][0] == MAIN_QUEUE


class TestTopology:
    async def test_retry_chain_is_exponential(self) -> None:
        policy = ExponentialBackoffRetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            factor=2.0,
        )

        first = retry_queue(
            1,
            policy,
            MAIN_QUEUE,
        )
        second = retry_queue(
            2,
            policy,
            MAIN_QUEUE,
        )

        assert first.name == retry_queue_name(
            MAIN_QUEUE,
            1,
        )
        assert first.arguments["x-message-ttl"] == 1000
        assert second.arguments["x-message-ttl"] == 2000
        # retry возвращается в свою же main-очередь
        assert first.arguments["x-dead-letter-routing-key"] == MAIN_QUEUE
        assert first.durable is True

    async def test_payment_pipeline_dead_letters_to_payment_dlq(self) -> None:
        queue = main_queue()

        assert queue.arguments["x-dead-letter-routing-key"] == DLQ_QUEUE
