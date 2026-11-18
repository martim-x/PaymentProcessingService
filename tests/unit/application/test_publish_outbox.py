"""Use case: PublishOutbox — relay публикует несент записи и помечает их."""

import pytest

from payment.application.dto import OutboxMessage
from payment.application.use_cases.publish_outbox import PublishOutbox
from tests.fakes import FakeMessagePublisher, FakeUnitOfWork


def message(aggregate_id: str = "p1") -> OutboxMessage:
    return OutboxMessage(
        id="6f9619ff-8b86-d011-b42d-00cf4fc964ff",
        aggregate_id=aggregate_id,
        payload={"event": "payment.created", "payment_id": aggregate_id},
    )


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

    async def test_broker_failure_leaves_record_unsent(self) -> None:
        uow = FakeUnitOfWork()
        await uow.outbox.add(payload={"event": "payment.created"}, aggregate_id="p1")
        publisher = FakeMessagePublisher(fail_first=1)
        use_case = PublishOutbox(uow=uow, publisher=publisher)

        with pytest.raises(RuntimeError):
            await use_case.execute(batch=10)

        unsent = await uow.outbox.fetch_unsent(batch=10)
        assert len(unsent) == 1

    async def test_empty_outbox_publishes_nothing(self) -> None:
        uow = FakeUnitOfWork()
        use_case = PublishOutbox(uow=uow, publisher=FakeMessagePublisher())

        published = await use_case.execute(batch=10)

        assert published == 0
