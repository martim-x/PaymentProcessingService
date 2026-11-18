"""Use case: CreatePayment — идемпотентное создание платежа."""

from decimal import Decimal

import pytest

from payment.application.dto import CreatePaymentCommand
from payment.application.exceptions import IdempotencyConflict
from payment.application.use_cases.create_payment import CreatePayment
from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import IdempotencyKey
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy
from tests.fakes import FakeGateway, FakeUnitOfWork, FakeWebhookSender


def command(idempotency_key: str = "key-1") -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Test payment",
        metadata={"order_id": "42"},
        idempotency_key=IdempotencyKey(idempotency_key),
        webhook_url="https://example.com/hook",
        fingerprint="deadbeef",
    )


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

        conflicting = command()
        object.__setattr__(conflicting, "fingerprint", "different")

        with pytest.raises(IdempotencyConflict):
            await use_case.execute(conflicting)


class TestProcessPayment:
    async def test_success_marks_succeeded(self) -> None:
        uow = FakeUnitOfWork()
        create = CreatePayment(uow=uow)
        created = await create.execute(command())
        uow.committed = 0

        from payment.application.use_cases.process_payment import ProcessPayment

        gateway = FakeGateway(succeed=True)
        webhook = FakeWebhookSender()
        use_case = ProcessPayment(uow=uow, gateway=gateway, webhook_sender=webhook)

        outcome = await use_case.execute(created.payment.id)

        assert outcome.status == "succeeded"
        assert uow.committed == 1
        assert webhook.calls == 1
        assert webhook.sent[0]["payload"]["status"] == "succeeded"

    async def test_failure_marks_failed_reason_kept(self) -> None:
        uow = FakeUnitOfWork()
        create = CreatePayment(uow=uow)
        created = await create.execute(command())
        uow.committed = 0

        from payment.application.use_cases.process_payment import ProcessPayment

        gateway = FakeGateway(succeed=False, reason="gateway said no")
        webhook = FakeWebhookSender()
        use_case = ProcessPayment(uow=uow, gateway=gateway, webhook_sender=webhook)

        outcome = await use_case.execute(created.payment.id)

        assert outcome.status == "failed"
        assert outcome.failure_reason == "gateway said no"

    async def test_webhook_failure_does_not_change_payment_status(self) -> None:
        uow = FakeUnitOfWork()
        create = CreatePayment(uow=uow)
        created = await create.execute(command())
        uow.committed = 0

        from payment.application.use_cases.process_payment import ProcessPayment

        gateway = FakeGateway(succeed=True)
        webhook = FakeWebhookSender(fail_first=99)  # всегда падает
        # нулевая задержка: тест не должен ждать экспоненциальный backoff
        fast_policy = ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=0.0, factor=1.0)
        use_case = ProcessPayment(
            uow=uow, gateway=gateway, webhook_sender=webhook, retry_policy=fast_policy
        )

        outcome = await use_case.execute(created.payment.id)

        assert outcome.status == "succeeded"
        assert webhook.calls > 0  # ретраились

    async def test_unknown_payment_raises(self) -> None:
        from payment.application.exceptions import PaymentNotFound
        from payment.application.use_cases.process_payment import ProcessPayment
        from payment.domain.model.value_objects import PaymentId

        uow = FakeUnitOfWork()
        use_case = ProcessPayment(
            uow=uow, gateway=FakeGateway(), webhook_sender=FakeWebhookSender()
        )

        with pytest.raises(PaymentNotFound):
            await use_case.execute(PaymentId.next())
