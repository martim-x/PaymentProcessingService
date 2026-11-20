"""OutboxService: публикация батча с изоляцией «ядовитых» записей."""

import pytest

from payment.application.ports.message_publisher import PublisherUnavailable
from payment.application.services.outbox_service import OutboxService
from tests.fakes import FakeMessagePublisher, FakeUnitOfWork

POISON = {"event": "payment.created", "payment_id": "poison"}


class TestOutboxService:
    async def test_publishes_and_marks_sent(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"event": "payment.created"}, aggregate_id="p1")
        publisher = FakeMessagePublisher()
        service = OutboxService(uow=uow, publisher=publisher)

        published = await service.publish_pending(batch=10)

        assert published == 1
        assert len(publisher.published) == 1
        assert publisher.published[0][0] == "payments.new"
        assert (await uow.outbox.fetch_unsent(batch=10)) == []

    async def test_broker_failure_keeps_record_unsent_and_counts_attempt(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"event": "payment.created"}, aggregate_id="p1")
        publisher = FakeMessagePublisher(fail_first=1)
        service = OutboxService(uow=uow, publisher=publisher, max_attempts=5)

        published = await service.publish_pending(batch=10)

        assert published == 0
        unsent = await uow.outbox.fetch_unsent(batch=10)
        assert len(unsent) == 1
        assert unsent[0].attempts == 1

    async def test_poison_record_does_not_block_others(self) -> None:
        """«Ядовитая» запись в середине батча не мешает остальным уйти."""
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"n": 1}, aggregate_id="a1")
        await uow.outbox.add(payload=POISON, aggregate_id="a2")
        await uow.outbox.add(payload={"n": 3}, aggregate_id="a3")
        publisher = FakeMessagePublisher(poison_payload=POISON)
        service = OutboxService(uow=uow, publisher=publisher, max_attempts=5)

        published = await service.publish_pending(batch=10)

        assert published == 2
        payloads = [payload["n"] for _, payload in publisher.published]
        assert payloads == [1, 3]
        unsent = await uow.outbox.fetch_unsent(batch=10)
        assert [record.payload for record in unsent] == [POISON]
        assert unsent[0].attempts == 1

    async def test_poison_record_becomes_failed_after_max_attempts(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload=POISON, aggregate_id="a1")
        publisher = FakeMessagePublisher(poison_payload=POISON)
        service = OutboxService(uow=uow, publisher=publisher, max_attempts=2)

        await service.publish_pending(batch=10)
        await service.publish_pending(batch=10)

        assert await uow.outbox.fetch_unsent(batch=10) == []
        all_records = uow.outbox.all_records()
        assert all_records[0].failed is True
        assert all_records[0].attempts == 2

    async def test_empty_outbox_publishes_nothing(self) -> None:
        service = OutboxService(uow=FakeUnitOfWork(), publisher=FakeMessagePublisher())

        assert await service.publish_pending(batch=10) == 0


class TestBrokerOutage:
    """Недоступность брокера не должна «сжигать» лимит попыток записей."""

    async def test_outage_does_not_increment_attempts(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"n": 1}, aggregate_id="a1")
        await uow.outbox.add(payload={"n": 2}, aggregate_id="a2")
        publisher = FakeMessagePublisher(unavailable_first=99)
        service = OutboxService(uow=uow, publisher=publisher, max_attempts=2)

        with pytest.raises(PublisherUnavailable):
            await service.publish_pending(batch=10)

        unsent = await uow.outbox.fetch_unsent(batch=10)
        assert len(unsent) == 2
        assert all(record.attempts == 0 for record in unsent)
        assert all(record.failed is False for record in unsent)

    async def test_partial_batch_is_committed_before_outage(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"n": 1}, aggregate_id="a1")
        await uow.outbox.add(payload={"n": 2}, aggregate_id="a2")
        publisher = FakeMessagePublisher(unavailable_first=0)
        calls = {"n": 0}
        original = publisher.publish

        async def publish_then_die(*, subject, payload, message_id=None):
            calls["n"] += 1
            if calls["n"] == 2:
                raise PublisherUnavailable("broker is down")
            await original(subject=subject, payload=payload, message_id=message_id)

        publisher.publish = publish_then_die
        service = OutboxService(uow=uow, publisher=publisher, max_attempts=2)

        with pytest.raises(PublisherUnavailable):
            await service.publish_pending(batch=10)

        unsent = await uow.outbox.fetch_unsent(batch=10)
        assert [record.payload["n"] for record in unsent] == [2]
        assert unsent[0].attempts == 0

    async def test_failed_attempt_is_committed_before_following_outage(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload=POISON, aggregate_id="poison")
        await uow.outbox.add(payload={"n": 2}, aggregate_id="a2")
        publisher = FakeMessagePublisher(poison_payload=POISON, unavailable_first=0)
        calls = {"n": 0}
        original = publisher.publish

        async def poison_then_die(*, subject, payload, message_id=None):
            calls["n"] += 1
            if calls["n"] == 2:
                raise PublisherUnavailable("broker is down")
            await original(subject=subject, payload=payload, message_id=message_id)

        publisher.publish = poison_then_die
        service = OutboxService(uow=uow, publisher=publisher, max_attempts=2)

        with pytest.raises(PublisherUnavailable):
            await service.publish_pending(batch=10)

        records = await uow.outbox.fetch_unsent(batch=10)
        assert len(records) == 2
        assert records[0].attempts == 1
        assert records[1].attempts == 0
        assert uow.committed == 1

    async def test_mark_sent_database_error_is_not_counted_as_publish_failure(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"n": 1}, aggregate_id="a1")
        publisher = FakeMessagePublisher()
        mark_sent = uow.outbox.mark_sent

        async def fail_mark_sent(record_id):
            raise RuntimeError("database write failed")

        uow.outbox.mark_sent = fail_mark_sent
        service = OutboxService(uow=uow, publisher=publisher, max_attempts=2)

        with pytest.raises(RuntimeError, match="database write failed"):
            await service.publish_pending(batch=10)

        records = uow.outbox.all_records()
        assert records[0].attempts == 0
        assert records[0].failed is False
        assert len(publisher.published) == 1
        uow.outbox.mark_sent = mark_sent
