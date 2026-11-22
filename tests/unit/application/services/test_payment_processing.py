"""PaymentService: идемпотентное создание, обработка и claim-сценарии на fake-портах."""

from decimal import Decimal

import pytest

from payment.application.dto import CreatePaymentCommand
from payment.application.event_types import OutboxEventType
from payment.application.exceptions import (
    IdempotencyConflict,
    PaymentBusy,
    PaymentNotFound,
)
from payment.application.ports.payment_gateway import PaymentGateway
from payment.application.ports.repositories import OutboxDispatch
from payment.application.services.outbox_service import OutboxService
from payment.application.services.payment_service import PaymentService
from payment.domain.model.enums import Currency, PaymentStatus
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import (
    IdempotencyKey,
    Money,
    PaymentId,
    WebhookUrl,
)
from tests.fakes import FakeGateway, FakeMessagePublisher, FakeUnitOfWork

pytestmark = [
    pytest.mark.unit,
    pytest.mark.application,
]


GATEWAY_REASON = "gateway said no"


def command(
    idempotency_key: str = "key-1",
    amount: Decimal = Decimal("100.00"),
    description: str = "Test payment",
) -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=amount,
        currency=Currency.RUB,
        description=description,
        metadata={"order_id": "42"},
        idempotency_key=IdempotencyKey(value=idempotency_key),
        webhook_url="https://example.com/hook",
    )


def webhook_events(uow: FakeUnitOfWork) -> list[dict]:
    return [
        r.payload
        for r in uow.outbox.all_records()
        if r.payload.get("event") == OutboxEventType.PAYMENT_WEBHOOK.value
    ]


def stored_event_id(uow: FakeUnitOfWork) -> str:
    """event_id единственной сохранённой webhook-команды."""
    events = webhook_events(uow)
    assert len(events) == 1, events
    return str(events[0]["event_id"])


def service(
    uow: FakeUnitOfWork,
    gateway: PaymentGateway | None = None,
    *,
    claim_ttl_seconds: float = 30.0,
    claim_wait_seconds: float = 0.0,
    claim_poll_seconds: float = 0.05,
) -> PaymentService:
    return PaymentService(
        uow=uow,
        gateway=gateway or FakeGateway(),
        claim_ttl_seconds=claim_ttl_seconds,
        claim_wait_seconds=claim_wait_seconds,
        claim_poll_seconds=claim_poll_seconds,
    )


class TestCreatePayment:
    async def test_creates_pending_payment_with_outbox_record(self) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(uow)

        result = await payment_service.create_payment(command=command())

        assert result.created is True
        assert result.payment.status.value == "pending"
        assert len(uow.outbox.all_records()) == 1
        record = uow.outbox.all_records()[0]
        assert record.payload["payment_id"] == str(result.payment.id.value)
        assert record.payload["event"] == OutboxEventType.PAYMENT_CREATED.value
        assert record.aggregate_id == str(result.payment.id.value)
        assert uow.committed == 1

    async def test_repeated_same_key_returns_same_payment_without_new_outbox(
        self,
    ) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(uow)

        first = await payment_service.create_payment(command=command())
        second = await payment_service.create_payment(command=command())

        assert second.created is False
        assert second.payment.id.value == first.payment.id.value
        assert len(uow.outbox.all_records()) == 1

    async def test_same_key_different_body_raises_conflict(self) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(uow)
        await payment_service.create_payment(command=command())

        # другой body при том же ключе -> другой fingerprint -> 409
        with pytest.raises(IdempotencyConflict):
            await payment_service.create_payment(
                command=command(amount=Decimal("999.00"))
            )

    async def test_integrity_error_race_returns_existing_payment(self) -> None:
        """Два одновременных запроса: проигравший получает существующий платёж, не 500."""
        uow = FakeUnitOfWork()
        payment_service = service(uow)
        winner = await payment_service.create_payment(command=command())

        # Первый SELECT не видит конкурирующую незакоммиченную строку.
        uow.payments.race_reads = [
            None,
            winner.payment,
        ]
        uow.payments.fail_next_add_with_integrity = True
        loser = await payment_service.create_payment(command=command())

        assert loser.created is False
        assert loser.payment.id.value == winner.payment.id.value
        assert uow.payments.add_calls == 2  # winner + race loser attempted INSERT
        assert uow.payments.idempotency_reads == 3  # seed + initial miss + reread

    async def test_integrity_error_race_with_different_body_is_conflict(self) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(uow)
        winner = await payment_service.create_payment(command=command())

        # При первом чтении конкурирующий платёж ещё не виден; после INSERT-конфликта виден.
        uow.payments.race_reads = [
            None,
            winner.payment,
        ]
        uow.payments.fail_next_add_with_integrity = True
        # другой body при том же ключе -> другой fingerprint -> 409
        with pytest.raises(IdempotencyConflict):
            await payment_service.create_payment(
                command=command(amount=Decimal("999.00"))
            )
        assert uow.payments.add_calls == 2  # winner + conflicting race INSERT
        assert uow.payments.idempotency_reads == 3  # seed + initial miss + reread


class TestPaymentServiceProcessing:
    async def test_success_marks_succeeded(self) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())

        outcome = await payment_service.process_payment(payment_id=created.payment.id)

        assert outcome.status == "succeeded"

    async def test_success_enqueues_webhook_task_atomically(self) -> None:
        """Финальный статус и outbox-событие пишутся одной транзакцией."""
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())
        commits_before = uow.committed

        await payment_service.process_payment(payment_id=created.payment.id)

        events = webhook_events(uow)
        assert len(events) == 1
        event = events[0]
        assert event["payment_id"] == str(created.payment.id)
        assert event["webhook_url"] == "https://example.com/hook"
        assert event["body"]["status"] == "succeeded"
        assert event["body"]["id"] == str(created.payment.id)
        assert event["body"]["event_id"] == event["event_id"]
        assert uow.committed - commits_before == 2

    async def test_processing_service_does_not_send_webhook(self) -> None:
        """HTTP-доставка — отдельная ответственность WebhookService."""
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())

        outcome = await payment_service.process_payment(payment_id=created.payment.id)

        assert outcome.status == "succeeded"
        assert uow.webhook_deliveries.delivered == []

    async def test_webhook_record_is_local_and_not_marked_sent(self) -> None:
        """Запись для consumer'а не притворяется опубликованной и не уходит в relay."""
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())

        await payment_service.process_payment(payment_id=created.payment.id)

        stored = [
            r
            for r in uow.outbox.all_records()
            if r.event_type == OutboxEventType.PAYMENT_WEBHOOK.value
        ]
        assert len(stored) == 1
        assert stored[0].dispatch == OutboxDispatch.LOCAL
        assert stored[0].sent is False

        publisher = FakeMessagePublisher()
        published = await OutboxService(
            uow=uow,
            publisher=publisher,
            subject="payments.new",
        ).publish_pending(batch=10)
        # relay публикует только payment.created, запись webhook не покидает БД
        assert published == 1
        assert [subject for subject, _ in publisher.published] == ["payments.new"]

    async def test_crash_after_finalize_redelivery_sends_webhook_without_gateway(
        self,
    ) -> None:
        """Падение между финализацией и webhook: повтор берёт готовую команду из БД."""
        uow = FakeUnitOfWork()
        first_gateway = FakeGateway(succeed=True)
        created = await service(
            uow,
            first_gateway,
        ).create_payment(command=command())

        outcome = await service(
            uow,
            first_gateway,
        ).process_payment(payment_id=created.payment.id)
        assert outcome.webhook_delivery is not None  # процесс «упал» до HTTP-отправки

        replay_gateway = FakeGateway(succeed=True)
        replay = await service(
            uow,
            replay_gateway,
        ).process_payment(payment_id=created.payment.id)

        assert replay_gateway.calls == 0
        assert replay.status == "succeeded"
        assert replay.webhook_delivery == outcome.webhook_delivery
        assert replay.webhook_delivery is not None
        assert replay.webhook_delivery.event_id == stored_event_id(uow)

    async def test_failure_marks_failed_reason_kept(self) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(
                succeed=False,
                reason=GATEWAY_REASON,
            ),
        )
        created = await payment_service.create_payment(command=command())

        outcome = await payment_service.process_payment(payment_id=created.payment.id)

        assert outcome.status == "failed"
        assert outcome.failure_reason == GATEWAY_REASON
        assert webhook_events(uow)[0]["body"]["status"] == "failed"

    async def test_duplicate_message_does_not_enqueue_second_webhook(self) -> None:
        """Повтор сообщения после финализации не вызывает шлюз второй раз."""
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())
        await payment_service.process_payment(payment_id=created.payment.id)

        gateway = FakeGateway(succeed=True)
        duplicate_service = service(
            uow,
            gateway,
        )
        outcome = await duplicate_service.process_payment(payment_id=created.payment.id)

        replay_gateway = FakeGateway(succeed=True)
        replay = await service(
            uow,
            replay_gateway,
        ).process_payment(payment_id=created.payment.id)

        assert gateway.calls == 0
        assert replay_gateway.calls == 0
        events = webhook_events(uow)
        assert len(events) == 1
        assert outcome.status == "succeeded"
        assert outcome.webhook_delivery is not None
        assert replay.webhook_delivery == outcome.webhook_delivery
        assert outcome.webhook_delivery.event_id == events[0]["event_id"]
        assert outcome.webhook_delivery.body == events[0]["body"]
        assert outcome.webhook_delivery.url == events[0]["webhook_url"]

    @pytest.mark.parametrize(
        "persisted_event",
        [
            None,
            {
                "event": OutboxEventType.PAYMENT_WEBHOOK.value,
                "event_id": 3,
            },
        ],
    )
    async def test_final_payment_without_valid_persisted_webhook_returns_no_delivery(
        self,
        persisted_event: dict | None,
    ) -> None:
        """Missing/malformed persisted payload must be surfaced for consumer retry."""
        uow = FakeUnitOfWork()
        gateway = FakeGateway(succeed=True)
        payment_service = service(
            uow,
            gateway,
        )
        created = await payment_service.create_payment(command=command())
        await payment_service.process_payment(payment_id=created.payment.id)

        records = [
            r
            for r in uow.outbox.all_records()
            if r.payload.get("event") == OutboxEventType.PAYMENT_WEBHOOK.value
        ]
        if persisted_event is None:
            uow.outbox._records.remove(records[0])
        else:
            records[0].payload = persisted_event

        replay = await service(
            uow,
            FakeGateway(succeed=True),
        ).process_payment(payment_id=created.payment.id)

        assert replay.webhook_delivery is None
        assert gateway.calls == 1

    async def test_concurrent_claim_blocks_second_gateway_call(self) -> None:
        """Чужой claim откладывает обработку, не вызывая шлюз."""
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())
        pid = created.payment.id
        uow.payments.claimed.add(str(pid))
        gateway = FakeGateway(succeed=True)
        duplicate_service = service(
            uow,
            gateway,
            claim_wait_seconds=0.0,
        )

        with pytest.raises(PaymentBusy):
            await duplicate_service.process_payment(payment_id=pid)

        assert gateway.calls == 0

    async def test_gateway_receives_idempotency_key(self) -> None:
        uow = FakeUnitOfWork()
        gateway = FakeGateway(succeed=True)
        payment_service = service(
            uow,
            gateway,
        )
        created = await payment_service.create_payment(command=command())

        await payment_service.process_payment(payment_id=created.payment.id)

        assert gateway.last_idempotency_key == str(created.payment.id)

    async def test_unknown_payment_raises(self) -> None:
        payment_service = service(FakeUnitOfWork())

        with pytest.raises(PaymentNotFound):
            await payment_service.process_payment(payment_id=PaymentId.next())


class TestOutboxTransactionality:
    """Сбой INSERT в outbox обязан откатить и статус, и снять клейм."""

    async def test_outbox_failure_rolls_back_final_status(self) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())
        pid = created.payment.id
        uow.outbox.fail_next_add = True

        with pytest.raises(
            RuntimeError,
            match="outbox insert failed",
        ):
            await payment_service.process_payment(payment_id=pid)

        stored = await uow.payments.get(payment_id=pid)
        assert stored is not None
        assert stored.status is PaymentStatus.PENDING
        assert stored.processed_at is None
        assert webhook_events(uow) == []
        # клейм снят: следующая доставка сможет обработать платёж
        assert str(pid) not in uow.payments.claimed

    async def test_retry_succeeds_after_outbox_failure(self) -> None:
        uow = FakeUnitOfWork()
        payment_service = service(
            uow,
            FakeGateway(succeed=True),
        )
        created = await payment_service.create_payment(command=command())
        pid = created.payment.id
        uow.outbox.fail_next_add = True

        with pytest.raises(RuntimeError):
            await payment_service.process_payment(payment_id=pid)

        outcome = await payment_service.process_payment(payment_id=pid)

        assert outcome.status == "succeeded"
        assert len(webhook_events(uow)) == 1


class TestLostClaim:
    """Потеря клейма не должна выдавать локальный результат шлюза за состояние БД."""

    async def test_lost_claim_while_pending_raises_busy(self) -> None:
        uow = FakeUnitOfWork()
        gateway = FakeGateway(succeed=True)
        payment_service = service(
            uow,
            gateway,
            claim_wait_seconds=0.0,
        )
        created = await payment_service.create_payment(command=command())
        pid = created.payment.id
        # клейм ушёл другому consumer'у: наш try_finalize вернёт False,
        # а платёж в БД остаётся pending
        uow.payments.finalized_elsewhere.add(str(pid))

        with pytest.raises(PaymentBusy):
            await payment_service.process_payment(payment_id=pid)

        assert gateway.calls == 1  # шлюз был вызван, но статус не наш
        stored = await uow.payments.get(payment_id=pid)
        assert stored is not None and stored.status is PaymentStatus.PENDING
        assert webhook_events(uow) == []

    async def test_lost_claim_returns_actual_terminal_state(self) -> None:
        uow = FakeUnitOfWork()
        created = await service(
            uow,
            FakeGateway(),
        ).create_payment(command=command())
        pid = created.payment.id

        # другой consumer успел записать failed, пока мы ходили в шлюз
        other = Payment(
            id=PaymentId(value=pid.value),
            amount=Money(
                amount=Decimal("100.00"),
                currency=Currency.RUB,
            ),
            description="Test payment",
            metadata={"order_id": "42"},
            idempotency_key=IdempotencyKey(value="key-1"),
            webhook_url=WebhookUrl(value="https://example.com/hook"),
        )
        other.mark_failed(reason="other consumer rejected")
        uow.payments.put(payment=other)
        uow.payments.finalized_elsewhere.add(str(pid))

        gateway = FakeGateway(succeed=True)
        payment_service = service(
            uow,
            gateway,
            claim_wait_seconds=0.0,
        )
        outcome = await payment_service.process_payment(payment_id=pid)

        # НЕ "succeeded" из локального результата шлюза, а фактическое состояние
        assert outcome.status == "failed"
        assert outcome.failure_reason == "other consumer rejected"
        assert webhook_events(uow) == []


class TestClaimFailureHandling:
    """Главный failure mode: сбой после клейма не должен «съесть» все ретраи."""

    async def test_claim_released_when_gateway_fails(self) -> None:
        uow = FakeUnitOfWork()
        created = await service(
            uow,
            FakeGateway(),
        ).create_payment(command=command())
        pid = created.payment.id

        failing_gateway = FakeGateway(fail_times=1)
        first = service(
            uow,
            failing_gateway,
        )

        with pytest.raises(
            RuntimeError,
            match="temporarily unavailable",
        ):
            await first.process_payment(payment_id=pid)

        # клейм снят — повторная попытка снова может взять платёж
        second_gateway = FakeGateway(succeed=True)
        second = service(
            uow,
            second_gateway,
        )
        outcome = await second.process_payment(payment_id=pid)

        assert outcome.status == "succeeded"
        assert second_gateway.calls == 1

    async def test_busy_claim_raises_instead_of_silent_ack(self) -> None:
        uow = FakeUnitOfWork()
        created = await service(
            uow,
            FakeGateway(),
        ).create_payment(command=command())
        pid = created.payment.id
        uow.payments.claimed.add(str(pid))  # другой consumer держит клейм

        payment_service = service(
            uow,
            FakeGateway(succeed=True),
            claim_wait_seconds=0.0,
        )

        with pytest.raises(PaymentBusy):
            await payment_service.process_payment(payment_id=pid)


class TestClaimReleaseOnCancellation:
    """SIGTERM -> CancelledError (BaseException): клейм обязан сняться сразу."""

    async def test_claim_released_when_task_cancelled(self) -> None:
        import asyncio

        uow = FakeUnitOfWork()
        created = await service(
            uow,
            FakeGateway(),
        ).create_payment(command=command())
        pid = created.payment.id

        class SlowGateway:
            started = asyncio.Event()

            async def process(
                self,
                *,
                payment_id,
                amount,
                currency,
                idempotency_key="",
            ):
                self.started.set()
                # «зависший» шлюз без таймера: корутина живёт до отмены задачи
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        gateway = SlowGateway()
        payment_service = service(
            uow,
            gateway,
        )

        task = asyncio.create_task(payment_service.process_payment(payment_id=pid))
        await gateway.started.wait()
        assert str(pid) in uow.payments.claimed  # клейм взят

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # клейм снят, хотя исключение — BaseException, а не Exception
        assert str(pid) not in uow.payments.claimed


class TestClaimWaiting:
    """Дубликат дожидается освобождения клейма вместо сжигания retry-попыток."""

    async def test_duplicate_waits_and_then_processes(self) -> None:
        import asyncio

        uow = FakeUnitOfWork()
        created = await service(
            uow,
            FakeGateway(),
        ).create_payment(command=command())
        pid = created.payment.id
        uow.payments.claimed.add(str(pid))  # первый consumer в работе

        async def release_soon() -> None:
            await asyncio.sleep(0.05)
            uow.payments.claimed.discard(str(pid))

        asyncio.create_task(release_soon())
        gateway = FakeGateway(succeed=True)
        payment_service = service(
            uow,
            gateway,
            claim_wait_seconds=1.0,
            claim_poll_seconds=0.01,
        )

        outcome = await payment_service.process_payment(payment_id=pid)

        assert outcome.status == "succeeded"
        assert gateway.calls == 1

    async def test_duplicate_gives_up_after_wait(self) -> None:
        uow = FakeUnitOfWork()
        created = await service(
            uow,
            FakeGateway(),
        ).create_payment(command=command())
        pid = created.payment.id
        uow.payments.claimed.add(str(pid))

        payment_service = service(
            uow,
            FakeGateway(succeed=True),
            claim_wait_seconds=0.05,
            claim_poll_seconds=0.01,
        )

        with pytest.raises(PaymentBusy):
            await payment_service.process_payment(payment_id=pid)

    async def test_terminal_payment_returns_instead_of_waiting(self) -> None:
        uow = FakeUnitOfWork()
        created = await service(
            uow,
            FakeGateway(),
        ).create_payment(command=command())
        pid = created.payment.id
        # платёж уже обработан другим consumer'ом
        created.payment.mark_succeeded()
        uow.payments.finalized_elsewhere.add(str(pid))

        payment_service = service(
            uow,
            FakeGateway(succeed=True),
            claim_wait_seconds=5.0,
            claim_poll_seconds=0.01,
        )

        outcome = await payment_service.process_payment(payment_id=pid)

        assert outcome.status == "succeeded"
