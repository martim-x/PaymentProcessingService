"""WebhookService: отдельная доставка, дедупликация по event_id, at-least-once."""

import uuid

import pytest

from payment.application.dto import WebhookDeliveryCommand
from payment.application.exceptions import (
    WebhookBusy,
    WebhookDeliveryExhausted,
    WebhookDeliveryFailed,
)
from payment.application.ports.repositories import WebhookDeliveryStatus
from payment.application.services.webhook_service import WebhookService
from tests.fakes import FakeUnitOfWork, FakeWebhookSender

pytestmark = [
    pytest.mark.unit,
    pytest.mark.application,
]


PAYMENT_ID = uuid.uuid4()


def command(event_id: str = "evt-1") -> WebhookDeliveryCommand:
    return WebhookDeliveryCommand(
        event_id=event_id,
        payment_id=PAYMENT_ID,
        url="https://example.com/hook",
        body={
            "id": str(PAYMENT_ID),
            "event": "payment.succeeded",
            "event_id": event_id,
            "status": "succeeded",
        },
    )


def service(
    uow: FakeUnitOfWork,
    sender: FakeWebhookSender,
    max_attempts: int = 3,
) -> WebhookService:
    return WebhookService(
        uow=uow,
        sender=sender,
        max_attempts=max_attempts,
    )


class TestSuccessfulDelivery:
    async def test_marks_delivered_and_sends_once(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender()

        result = await service(
            uow,
            sender,
        ).deliver(command=command())

        assert result.status == WebhookDeliveryStatus.DELIVERED
        assert result.already_delivered is False
        assert sender.calls == 1
        assert sender.sent[0]["url"] == "https://example.com/hook"
        record = await uow.webhook_deliveries.get(event_id="evt-1")
        assert record is not None and record.status == WebhookDeliveryStatus.DELIVERED

    async def test_duplicate_message_after_delivery_is_not_resent(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender()
        await service(
            uow,
            sender,
        ).deliver(command=command())

        result = await service(
            uow,
            sender,
        ).deliver(command=command())

        assert result.already_delivered is True
        assert sender.calls == 1

    async def test_event_id_is_part_of_signed_body(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender()

        await service(
            uow,
            sender,
        ).deliver(command=command("evt-stable"))

        assert sender.sent[0]["payload"]["event_id"] == "evt-stable"


class TestRetries:
    async def test_failed_attempt_is_counted_and_left_pending(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender(fail_first=1)

        with pytest.raises(WebhookDeliveryFailed):
            await service(
                uow,
                sender,
            ).deliver(command=command())

        record = await uow.webhook_deliveries.get(event_id="evt-1")
        assert record is not None
        assert record.attempts == 1
        assert record.status == "pending"
        assert "unreachable" in (record.last_error or "")

    async def test_retry_after_failure_delivers(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender(fail_first=1)

        with pytest.raises(WebhookDeliveryFailed):
            await service(
                uow,
                sender,
            ).deliver(command=command())
        result = await service(
            uow,
            sender,
        ).deliver(command=command())

        assert result.status == WebhookDeliveryStatus.DELIVERED
        assert sender.calls == 2

    async def test_exhausted_delivery_becomes_terminal(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender(fail_first=99)

        with pytest.raises(WebhookDeliveryFailed):
            await service(
                uow,
                sender,
                max_attempts=2,
            ).deliver(command=command())
        with pytest.raises(WebhookDeliveryExhausted):
            await service(
                uow,
                sender,
                max_attempts=2,
            ).deliver(command=command())

        record = await uow.webhook_deliveries.get(event_id="evt-1")
        assert record is not None
        assert record.attempts == 2
        assert record.status == WebhookDeliveryStatus.FAILED
        # третьей отправки нет: задача мертва
        with pytest.raises(WebhookDeliveryExhausted):
            await service(
                uow,
                sender,
                max_attempts=2,
            ).deliver(command=command())
        assert sender.calls == 2

    async def test_concurrent_duplicate_is_rejected_while_claim_is_held(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender()
        await uow.webhook_deliveries.create_if_absent(
            event_id="evt-1",
            payment_id=PAYMENT_ID,
        )
        await uow.webhook_deliveries.try_claim(
            event_id="evt-1",
            ttl_seconds=60,
        )  # другой consumer в работе

        with pytest.raises(WebhookBusy):
            await service(
                uow,
                sender,
            ).deliver(command=command())

        assert sender.calls == 0

    async def test_crash_after_send_repeats_delivery_at_least_once(self) -> None:
        """Падение между HTTP-ответом и записью статуса даёт повторную отправку."""
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender()

        # первая попытка: webhook ушёл, но статус не записан (имитация crash)
        await uow.webhook_deliveries.create_if_absent(
            event_id="evt-1",
            payment_id=PAYMENT_ID,
        )
        await sender.send(
            url="https://example.com/hook",
            payload={"event_id": "evt-1"},
        )
        record = await uow.webhook_deliveries.get(event_id="evt-1")
        assert record is not None and record.status == "pending"

        await service(
            uow,
            sender,
        ).deliver(command=command())

        # at-least-once: получатель обязан дедуплицировать по event_id
        assert sender.calls == 2
        delivered = await uow.webhook_deliveries.get(event_id="evt-1")
        assert (
            delivered is not None
            and delivered.status == WebhookDeliveryStatus.DELIVERED
        )


class TestTaskLifecycle:
    async def test_same_event_uses_single_delivery_record(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender(fail_first=1)

        with pytest.raises(WebhookDeliveryFailed):
            await service(
                uow,
                sender,
            ).deliver(command=command())
        await service(
            uow,
            sender,
        ).deliver(command=command())

        assert len(uow.webhook_deliveries._records) == 1  # noqa: SLF001
