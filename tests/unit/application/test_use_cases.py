"""Use cases: идемпотентность и обработка платежа на fake-портах."""

from decimal import Decimal

import pytest

from payment.application.dto import CreatePaymentCommand
from payment.application.exceptions import IdempotencyConflict, PaymentBusy, PaymentNotFound
from payment.application.use_cases.create_payment import CreatePayment
from payment.application.use_cases.process_payment import ProcessPayment
from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import IdempotencyKey, PaymentId
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy
from tests.fakes import FakeGateway, FakeUnitOfWork, FakeWebhookSender


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
        idempotency_key=IdempotencyKey(idempotency_key),
        webhook_url="https://example.com/hook",
    )


def fast_policy() -> ExponentialBackoffRetryPolicy:
    """Нулевая задержка: тесты не должны ждать экспоненциальный backoff."""
    return ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=0.0, factor=1.0)


class TestCreatePayment:
    async def test_creates_pending_payment_with_outbox_record(self) -> None:
        uow = FakeUnitOfWork()
        use_case = CreatePayment(uow=uow)

        result = await use_case.execute(command())

        assert result.created is True
        assert result.payment.status.value == "pending"
        assert len(uow.outbox.all_records()) == 1
        record = uow.outbox.all_records()[0]
        assert record.payload["payment_id"] == str(result.payment.id.value)
        assert record.payload["event"] == "payment.created"
        assert record.aggregate_id == str(result.payment.id.value)
        assert uow.committed == 1

    async def test_repeated_same_key_returns_same_payment_without_new_outbox(self) -> None:
        uow = FakeUnitOfWork()
        use_case = CreatePayment(uow=uow)

        first = await use_case.execute(command())
        second = await use_case.execute(command())

        assert second.created is False
        assert second.payment.id.value == first.payment.id.value
        assert len(uow.outbox.all_records()) == 1

    async def test_same_key_different_body_raises_conflict(self) -> None:
        uow = FakeUnitOfWork()
        use_case = CreatePayment(uow=uow)
        await use_case.execute(command())

        # другой body при том же ключе -> другой fingerprint -> 409
        with pytest.raises(IdempotencyConflict):
            await use_case.execute(command(amount=Decimal("999.00")))

    async def test_integrity_error_race_returns_existing_payment(self) -> None:
        """Два одновременных запроса: проигравший получает существующий платёж, не 500."""
        uow = FakeUnitOfWork()
        use_case = CreatePayment(uow=uow)
        winner = await use_case.execute(command())

        # имитируем гонку: INSERT проигравшего падает на unique-индексе
        uow.payments.fail_next_add_with_integrity = True
        loser = await use_case.execute(command())

        assert loser.created is False
        assert loser.payment.id.value == winner.payment.id.value

    async def test_integrity_error_race_with_different_body_is_conflict(self) -> None:
        uow = FakeUnitOfWork()
        use_case = CreatePayment(uow=uow)
        await use_case.execute(command())

        uow.payments.fail_next_add_with_integrity = True
        # другой body при том же ключе -> другой fingerprint -> 409
        with pytest.raises(IdempotencyConflict):
            await use_case.execute(command(amount=Decimal("999.00")))


class TestProcessPayment:
    async def test_success_marks_succeeded(self) -> None:
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())

        webhook = FakeWebhookSender()
        use_case = ProcessPayment(
            uow=uow,
            gateway=FakeGateway(succeed=True),
            webhook_sender=webhook,
            retry_policy=fast_policy(),
        )

        outcome = await use_case.execute(created.payment.id)

        assert outcome.status == "succeeded"
        assert webhook.calls == 1
        assert webhook.sent[0]["payload"]["status"] == "succeeded"

    async def test_failure_marks_failed_reason_kept(self) -> None:
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())

        use_case = ProcessPayment(
            uow=uow,
            gateway=FakeGateway(succeed=False, reason="gateway said no"),
            webhook_sender=FakeWebhookSender(),
            retry_policy=fast_policy(),
        )

        outcome = await use_case.execute(created.payment.id)

        assert outcome.status == "failed"
        assert outcome.failure_reason == "gateway said no"

    async def test_duplicate_message_does_not_notify_twice(self) -> None:
        """Если платёж уже финализирован другим consumer'ом — webhook не дублируется."""
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())
        pid = created.payment.id

        first_webhook = FakeWebhookSender()
        first = ProcessPayment(
            uow=uow,
            gateway=FakeGateway(succeed=True),
            webhook_sender=first_webhook,
            retry_policy=fast_policy(),
        )
        await first.execute(pid)

        second_webhook = FakeWebhookSender()
        gateway = FakeGateway(succeed=True)
        second = ProcessPayment(
            uow=uow, gateway=gateway, webhook_sender=second_webhook, retry_policy=fast_policy()
        )
        outcome = await second.execute(pid)

        assert first_webhook.calls == 1
        assert second_webhook.calls == 0
        assert gateway.calls == 0  # уже финальный статус: шлюз не вызываем
        assert outcome.status == "succeeded"

    async def test_webhook_failure_does_not_change_payment_status(self) -> None:
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())

        webhook = FakeWebhookSender(fail_first=99)  # всегда падает
        use_case = ProcessPayment(
            uow=uow,
            gateway=FakeGateway(succeed=True),
            webhook_sender=webhook,
            retry_policy=fast_policy(),
        )

        outcome = await use_case.execute(created.payment.id)

        assert outcome.status == "succeeded"
        assert webhook.calls == 3  # max_attempts попыток

    async def test_concurrent_claim_blocks_second_gateway_call(self) -> None:
        """Другой consumer держит клейм: шлюз не вызывается вовсе (нет двойного списания)."""
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())
        pid = created.payment.id
        uow.payments.claimed.add(str(pid))  # имитируем активный клейм

        gateway = FakeGateway(succeed=True)
        webhook = FakeWebhookSender()
        use_case = ProcessPayment(
            uow=uow, gateway=gateway, webhook_sender=webhook, retry_policy=fast_policy()
        )

        # занято другим consumer'ом -> исключение (retry), а не молчаливый ack
        with pytest.raises(PaymentBusy):
            await use_case.execute(pid)

        assert gateway.calls == 0
        assert webhook.calls == 0

    async def test_gateway_receives_idempotency_key(self) -> None:
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())

        gateway = FakeGateway(succeed=True)
        use_case = ProcessPayment(
            uow=uow, gateway=gateway, webhook_sender=FakeWebhookSender(), retry_policy=fast_policy()
        )

        await use_case.execute(created.payment.id)

        assert gateway.last_idempotency_key == str(created.payment.id)

    async def test_unknown_payment_raises(self) -> None:
        use_case = ProcessPayment(
            uow=FakeUnitOfWork(),
            gateway=FakeGateway(),
            webhook_sender=FakeWebhookSender(),
            retry_policy=fast_policy(),
        )

        with pytest.raises(PaymentNotFound):
            await use_case.execute(PaymentId.next())


class TestClaimFailureHandling:
    """Главный failure mode: сбой после клейма не должен «съесть» все ретраи."""

    async def test_claim_released_when_gateway_fails(self) -> None:
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())
        pid = created.payment.id

        failing_gateway = FakeGateway(fail_times=1)
        first = ProcessPayment(
            uow=uow,
            gateway=failing_gateway,
            webhook_sender=FakeWebhookSender(),
            retry_policy=fast_policy(),
        )

        with pytest.raises(RuntimeError, match="temporarily unavailable"):
            await first.execute(pid)

        # клейм снят — повторная попытка снова может взять платёж
        second_gateway = FakeGateway(succeed=True)
        second = ProcessPayment(
            uow=uow,
            gateway=second_gateway,
            webhook_sender=FakeWebhookSender(),
            retry_policy=fast_policy(),
        )
        outcome = await second.execute(pid)

        assert outcome.status == "succeeded"
        assert second_gateway.calls == 1

    async def test_busy_claim_raises_instead_of_silent_ack(self) -> None:
        uow = FakeUnitOfWork()
        created = await CreatePayment(uow=uow).execute(command())
        pid = created.payment.id
        uow.payments.claimed.add(str(pid))  # другой consumer держит клейм

        use_case = ProcessPayment(
            uow=uow,
            gateway=FakeGateway(succeed=True),
            webhook_sender=FakeWebhookSender(),
            retry_policy=fast_policy(),
        )

        with pytest.raises(PaymentBusy):
            await use_case.execute(pid)
