"""Use case: идемпотентное создание платежа (API)."""

from payment.application.dto import CreatePaymentCommand, CreatePaymentResult
from payment.application.exceptions import IdempotencyConflict
from payment.application.ports.repositories import AbstractUnitOfWork
from payment.domain.events import PaymentCreated
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import Money, PaymentId, WebhookUrl


def _outbox_payload(event: PaymentCreated) -> dict:
    return {"event": "payment.created", "payment_id": str(event.payment_id)}


class CreatePayment:
    """POST /payments: дубли по Idempotency-Key не создаём.

    - тот же ключ + тот же fingerprint -> возвращаем существующий платёж
    - тот же ключ + другой fingerprint -> IdempotencyConflict (HTTP 409)
    - новый ключ -> платёж + запись outbox В ОДНОЙ транзакции (outbox pattern)
    """

    def __init__(self, *, uow: AbstractUnitOfWork) -> None:
        self._uow = uow

    async def execute(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        async with self._uow as uow:
            existing = await uow.payments.get_by_idempotency_key(command.idempotency_key.value)
            if existing is not None:
                if existing.fingerprint != command.fingerprint:
                    raise IdempotencyConflict(command.idempotency_key.value)
                return CreatePaymentResult(payment=existing, created=False)

            payment = Payment.create(
                payment_id=PaymentId.next(),
                amount=Money(amount=command.amount, currency=command.currency),
                description=command.description,
                metadata=command.metadata,
                idempotency_key=command.idempotency_key,
                webhook_url=WebhookUrl(command.webhook_url),
                fingerprint=command.fingerprint,
            )
            await uow.payments.add(payment)

            for event in payment.collect_events():
                if isinstance(event, PaymentCreated):
                    await uow.outbox.add(
                        payload=_outbox_payload(event),
                        aggregate_id=str(payment.id),
                    )

            await uow.commit()
            return CreatePaymentResult(payment=payment, created=True)
