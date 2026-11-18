"""Use case: PublishOutbox — публикация батча с изоляцией «ядовитых» записей."""

from payment.application.use_cases.publish_outbox import PublishOutbox
from tests.fakes import FakeMessagePublisher, FakeUnitOfWork

POISON = {"event": "payment.created", "payment_id": "poison"}


class TestPublishOutbox:
    async def test_publishes_and_marks_sent(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"event": "payment.created"}, aggregate_id="p1")
        publisher = FakeMessagePublisher()
        use_case = PublishOutbox(uow=uow, publisher=publisher)

        published = await use_case.execute(batch=10)

        assert published == 1
        assert len(publisher.published) == 1
        assert publisher.published[0][0] == "payments.new"
        assert (await uow.outbox.fetch_unsent(batch=10)) == []

    async def test_broker_failure_keeps_record_unsent_and_counts_attempt(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"event": "payment.created"}, aggregate_id="p1")
        publisher = FakeMessagePublisher(fail_first=1)
        use_case = PublishOutbox(uow=uow, publisher=publisher, max_attempts=5)

        published = await use_case.execute(batch=10)

        assert published == 0  # батч не упал, запись осталась несент
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
        use_case = PublishOutbox(uow=uow, publisher=publisher, max_attempts=5)

        published = await use_case.execute(batch=10)

        assert published == 2
        payloads = [payload["n"] for _, payload in publisher.published]
        assert payloads == [1, 3]
        unsent = await uow.outbox.fetch_unsent(batch=10)
        assert [r.payload for r in unsent] == [POISON]
        assert unsent[0].attempts == 1

    async def test_poison_record_becomes_failed_after_max_attempts(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload=POISON, aggregate_id="a1")
        publisher = FakeMessagePublisher(poison_payload=POISON)
        use_case = PublishOutbox(uow=uow, publisher=publisher, max_attempts=2)

        await use_case.execute(batch=10)
        await use_case.execute(batch=10)
        # после двух неудач запись помечена failed и больше не выбирается
        assert await uow.outbox.fetch_unsent(batch=10) == []
        all_records = uow.outbox.all_records()
        assert all_records[0].failed is True
        assert all_records[0].attempts == 2

    async def test_empty_outbox_publishes_nothing(self) -> None:
        uow = FakeUnitOfWork()
        use_case = PublishOutbox(uow=uow, publisher=FakeMessagePublisher())

        published = await use_case.execute(batch=10)

        assert published == 0
