"""Fencing token webhook-клейма: конкурирующие владельцы, stale-записи, SIGKILL.

Регрессии, которые эти тесты закрывают:
- `mark_delivered`/`mark_attempt_failed` без токена позволяли устаревшему
  владельцу перетереть состояние нового обработчика;
- потеря claim'а (WebhookBusy) сжигала retry-попытки раньше истечения lease,
  и уведомление уходило в DLQ недоставленным;
- отмена задачи оставляла клейм висеть до TTL.
"""

import asyncio
import uuid

import pytest

from payment.application.dto import WebhookDeliveryCommand
from payment.application.exceptions import (
    WebhookBusy,
    WebhookDeliveryExhausted,
    WebhookDeliveryFailed,
)
from payment.application.ports.repositories import (
    DELIVERY_DELIVERED,
    DELIVERY_DELIVERING,
    DELIVERY_PENDING,
)
from payment.application.services.webhook_service import WebhookService
from tests.fakes import FakeUnitOfWork, FakeWebhookSender

PAYMENT_ID = uuid.uuid4()
EVENT_ID = "evt-1"


def command() -> WebhookDeliveryCommand:
    return WebhookDeliveryCommand(
        event_id=EVENT_ID,
        payment_id=PAYMENT_ID,
        url="https://example.com/hook",
        body={"id": str(PAYMENT_ID), "event_id": EVENT_ID, "status": "succeeded"},
    )


def service(uow: FakeUnitOfWork, sender, max_attempts: int = 3) -> WebhookService:
    return WebhookService(uow=uow, sender=sender, claim_ttl_seconds=30.0, max_attempts=max_attempts)


class HookedSender:
    """Sender, который выполняет сценарий во время HTTP-запроса."""

    def __init__(self, hook=None, *, fail: bool = False) -> None:
        self._hook = hook
        self._fail = fail
        self.calls = 0

    async def send(self, *, url: str, payload: dict) -> None:
        self.calls += 1
        if self._hook is not None:
            await self._hook()
        if self._fail:
            raise RuntimeError("webhook endpoint unreachable")


class TestTwoCompetingOwners:
    async def test_second_owner_cannot_deliver_while_claim_is_held(self) -> None:
        uow = FakeUnitOfWork()
        await uow.webhook_deliveries.create_if_absent(event_id=EVENT_ID, payment_id=PAYMENT_ID)
        await uow.webhook_deliveries.try_claim(EVENT_ID, 60)  # владелец A в полёте

        sender_b = FakeWebhookSender()
        with pytest.raises(WebhookBusy):
            await service(uow, sender_b).deliver(command())

        assert sender_b.calls == 0
        record = await uow.webhook_deliveries.get(EVENT_ID)
        assert record is not None and record.attempts == 0

    async def test_stale_owner_success_does_not_overwrite_new_owner(self) -> None:
        """A отправил webhook, но клейм успел перейти к B: статус пишет только B."""
        uow = FakeUnitOfWork()
        sender_b = FakeWebhookSender()

        async def takeover() -> None:
            # TTL истёк, пока A ждал ответа: B берёт клейм и доставляет
            uow.webhook_deliveries.expire_claim(EVENT_ID)
            await service(uow, sender_b).deliver(command())

        sender_a = HookedSender(hook=takeover)
        result = await service(uow, sender_a).deliver(command())

        assert result.stale is True
        assert sender_a.calls == 1 and sender_b.calls == 1
        record = await uow.webhook_deliveries.get(EVENT_ID)
        assert record is not None
        assert record.status == DELIVERY_DELIVERED
        assert uow.webhook_deliveries.delivered == [EVENT_ID]

    async def test_stale_owner_failure_does_not_count_attempt(self) -> None:
        """Отказ устаревшего владельца не должен засчитываться новому."""
        uow = FakeUnitOfWork()
        sender_b = FakeWebhookSender()

        async def takeover() -> None:
            uow.webhook_deliveries.expire_claim(EVENT_ID)
            await service(uow, sender_b).deliver(command())

        sender_a = HookedSender(hook=takeover, fail=True)

        with pytest.raises(WebhookBusy):
            await service(uow, sender_a).deliver(command())

        record = await uow.webhook_deliveries.get(EVENT_ID)
        assert record is not None
        assert record.status == DELIVERY_DELIVERED
        assert record.attempts == 0
        assert record.last_error is None


class TestStaleFencing:
    async def test_stale_mark_delivered_is_rejected(self) -> None:
        uow = FakeUnitOfWork()
        repo = uow.webhook_deliveries
        await repo.create_if_absent(event_id=EVENT_ID, payment_id=PAYMENT_ID)
        token_a = await repo.try_claim(EVENT_ID, 0.0)
        assert token_a is not None
        repo.expire_claim(EVENT_ID)
        token_b = await repo.try_claim(EVENT_ID, 60.0)
        assert token_b is not None and token_b != token_a

        assert await repo.mark_delivered(EVENT_ID, token_a) is False

        record = await repo.get(EVENT_ID)
        assert record is not None
        assert record.status == DELIVERY_DELIVERING
        assert record.locked_until == token_b  # состояние нового владельца цело

    async def test_stale_mark_failed_is_rejected(self) -> None:
        uow = FakeUnitOfWork()
        repo = uow.webhook_deliveries
        await repo.create_if_absent(event_id=EVENT_ID, payment_id=PAYMENT_ID)
        token_a = await repo.try_claim(EVENT_ID, 0.0)
        assert token_a is not None
        repo.expire_claim(EVENT_ID)
        token_b = await repo.try_claim(EVENT_ID, 60.0)
        assert token_b is not None

        assert await repo.mark_attempt_failed(EVENT_ID, token_a, 3, "stale") is None

        record = await repo.get(EVENT_ID)
        assert record is not None
        assert record.attempts == 0
        assert record.last_error is None
        assert record.locked_until == token_b

    async def test_stale_release_does_not_drop_new_claim(self) -> None:
        uow = FakeUnitOfWork()
        repo = uow.webhook_deliveries
        await repo.create_if_absent(event_id=EVENT_ID, payment_id=PAYMENT_ID)
        token_a = await repo.try_claim(EVENT_ID, 0.0)
        assert token_a is not None
        repo.expire_claim(EVENT_ID)
        token_b = await repo.try_claim(EVENT_ID, 60.0)
        assert token_b is not None

        await repo.release_claim(EVENT_ID, token_a)

        assert repo.held_claim(EVENT_ID) == token_b
        record = await repo.get(EVENT_ID)
        assert record is not None and record.status == DELIVERY_DELIVERING


class TestCrashRecovery:
    async def test_sigkill_then_retry_after_lease_delivers(self) -> None:
        """SIGKILL оставляет клейм; ретрай после истечения TTL доставляет событие."""
        uow = FakeUnitOfWork()
        repo = uow.webhook_deliveries
        await repo.create_if_absent(event_id=EVENT_ID, payment_id=PAYMENT_ID)
        # владелец A захватил клейм и умер: ни release, ни записи статуса
        assert await repo.try_claim(EVENT_ID, 30.0) is not None
        repo.expire_claim(EVENT_ID)

        sender = FakeWebhookSender()
        result = await service(uow, sender).deliver(command())

        assert result.status == DELIVERY_DELIVERED
        assert sender.calls == 1
        record = await repo.get(EVENT_ID)
        assert record is not None
        assert record.attempts == 0  # WebhookBusy не считался попыткой
        assert record.status == DELIVERY_DELIVERED

    async def test_retry_before_lease_expiry_is_busy_not_dead(self) -> None:
        """Ранний дубликат при живом клейме не должен становиться terminal-ошибкой."""
        uow = FakeUnitOfWork()
        repo = uow.webhook_deliveries
        await repo.create_if_absent(event_id=EVENT_ID, payment_id=PAYMENT_ID)
        await repo.try_claim(EVENT_ID, 30.0)

        sender = FakeWebhookSender()
        with pytest.raises(WebhookBusy):  # не WebhookDeliveryExhausted
            await service(uow, sender).deliver(command())

        record = await repo.get(EVENT_ID)
        assert record is not None
        assert record.status == DELIVERY_DELIVERING
        assert record.attempts == 0

    async def test_repeated_busy_never_exhausts_attempts(self) -> None:
        """Сколько бы раз дубликат ни попадал на живой клейм, попытки не тратятся."""
        uow = FakeUnitOfWork()
        repo = uow.webhook_deliveries
        await repo.create_if_absent(event_id=EVENT_ID, payment_id=PAYMENT_ID)
        await repo.try_claim(EVENT_ID, 30.0)
        sender = FakeWebhookSender()

        for _ in range(5):
            with pytest.raises(WebhookBusy):
                await service(uow, sender, max_attempts=1).deliver(command())

        record = await repo.get(EVENT_ID)
        assert record is not None
        assert record.status == DELIVERY_DELIVERING
        assert record.attempts == 0


class TestCancellation:
    async def test_cancelled_task_releases_claim_with_token(self) -> None:
        uow = FakeUnitOfWork()
        repo = uow.webhook_deliveries
        started = asyncio.Event()

        class SlowSender:
            triggered = started

            async def send(self, *, url: str, payload: dict) -> None:
                self.triggered.set()
                await asyncio.Event().wait()  # живём до отмены

        task = asyncio.create_task(service(uow, SlowSender()).deliver(command()))
        await started.wait()
        assert repo.held_claim(EVENT_ID) is not None

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        record = await repo.get(EVENT_ID)
        assert record is not None
        assert record.status == DELIVERY_PENDING  # клейм снят, не ждём TTL
        assert repo.held_claim(EVENT_ID) is None
        assert record.attempts == 0  # отмена — не неудачная доставка

    async def test_retry_after_cancellation_needs_no_lease_wait(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender()
        started = asyncio.Event()

        class SlowSender:
            async def send(self, *, url: str, payload: dict) -> None:
                started.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(service(uow, SlowSender()).deliver(command()))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        result = await service(uow, sender).deliver(command())

        assert result.status == DELIVERY_DELIVERED
        assert sender.calls == 1


class TestExhaustion:
    async def test_attempts_are_counted_only_under_own_claim(self) -> None:
        uow = FakeUnitOfWork()
        sender = FakeWebhookSender(fail_first=99)

        for _ in range(2):
            with pytest.raises(WebhookDeliveryFailed):
                await service(uow, sender, max_attempts=3).deliver(command())

        with pytest.raises(WebhookDeliveryExhausted):
            await service(uow, sender, max_attempts=3).deliver(command())

        record = await uow.webhook_deliveries.get(EVENT_ID)
        assert record is not None
        assert record.attempts == 3
        assert record.status == "failed"
