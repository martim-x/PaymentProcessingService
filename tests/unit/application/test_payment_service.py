from decimal import Decimal

import pytest

from payment.application.dto import CreatePaymentCommand, PaymentDetails
from payment.application.exceptions import PaymentNotFound
from payment.application.services.payment_service import PaymentService
from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import IdempotencyKey, PaymentId
from tests.fakes import FakeGateway, FakeUnitOfWork


def command(*, amount: str = "12.50", key: str = "order-42") -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=Decimal(amount),
        currency=Currency.RUB,
        description="Order 42",
        metadata={"order_id": 42},
        idempotency_key=IdempotencyKey(key),
        webhook_url="https://example.com/webhook",
    )


class TestPaymentService:
    async def test_create_and_get_are_methods_of_the_same_service(self) -> None:
        service = PaymentService(uow=FakeUnitOfWork(), gateway=FakeGateway())

        created = await service.create_payment(command())
        loaded = await service.get_payment(created.payment.id)

        assert created.created is True
        assert isinstance(loaded, PaymentDetails)
        assert loaded.id == created.payment.id.value
        assert loaded.amount == Decimal("12.5000")

    async def test_get_raises_not_found(self) -> None:
        service = PaymentService(uow=FakeUnitOfWork(), gateway=FakeGateway())

        with pytest.raises(PaymentNotFound):
            await service.get_payment(PaymentId.next())

    async def test_process_is_method_of_payment_service(self) -> None:
        uow = FakeUnitOfWork()
        gateway = FakeGateway(succeed=True)
        service = PaymentService(uow=uow, gateway=gateway)
        created = await service.create_payment(command())

        result = await service.process_payment(created.payment.id)

        assert result.status == "succeeded"
        assert gateway.calls == 1
        events = [record.payload for record in uow.outbox.all_records()]
        assert any(event.get("event") == "payment.webhook" for event in events)
