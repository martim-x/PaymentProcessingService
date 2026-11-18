"""Use case: идемпотентное создание платежа (API)."""

import logging
from decimal import Decimal

from payment.application.dto import CreatePaymentCommand, CreatePaymentResult
from payment.application.exceptions import IdempotencyConflict, IdempotencyViolation
from payment.application.ports.repositories import AbstractUnitOfWork
from payment.domain.events import PaymentCreated
from payment.domain.model.fingerprint import compute_fingerprint
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import Money, PaymentId, WebhookUrl

logger = logging.getLogger(__name__)


def _outbox_payload(event: PaymentCreated) -> dict:
    return {"event": "payment.created", "payment_id": str(event.payment_id)}


def canonical_amount(amount: Decimal) -> str:
    """'100.5' и '100.50' — одна и та же сумма, один и тот же fingerprint."""
    return format(amount.normalize(), "f")


class CreatePayment:
    """POST /payments: дубли по Idempotency-Key не создаём.

    - тот же ключ + тот же fingerprint -> возвращаем существующий платёж
    - тот же ключ + другой fingerprint -> IdempotencyConflict (HTTP 409)
    - новый ключ -> платёж + запись outbox В ОДНОЙ транзакции (outbox pattern)

    Одновременные запросы с одним ключом разрешает unique-индекс: проигравший
    ловит IntegrityError и перечитывает платёж победителя (а не отдаёт 500).
    """

    def __init__(self, *, uow: AbstractUnitOfWork) -> None:
        self._uow = uow

    @staticmethod
    def _fingerprint(command: CreatePaymentCommand) -> str:
        """Бизнес-правило идемпотентности живёт здесь, а не в HTTP-слое."""
        return compute_fingerprint(
            {
                "amount": canonical_amount(command.amount),
                "currency": command.currency.value,
                "description": command.description,
                "metadata": dict(command.metadata),
                "webhook_url": command.webhook_url,
            }
        )

    async def execute(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        try:
            return await self._create(command)
        except IdempotencyViolation as exc:
            logger.info(
                "idempotency race for key %r; re-reading payment",
                command.idempotency_key.value,
            )
            return await self._read_existing(command, exc)

    async def _create(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        async with self._uow as uow:
            existing = await uow.payments.get_by_idempotency_key(command.idempotency_key.value)
            if existing is not None:
                return self._existing(command, existing)

            payment = Payment.create(
                payment_id=PaymentId.next(),
                amount=Money(amount=command.amount, currency=command.currency),
                description=command.description,
                metadata=command.metadata,
                idempotency_key=command.idempotency_key,
                webhook_url=WebhookUrl(command.webhook_url),
                fingerprint=self._fingerprint(command),
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

    async def _read_existing(
        self, command: CreatePaymentCommand, original: IdempotencyViolation
    ) -> CreatePaymentResult:
        async with self._uow as uow:
            existing = await uow.payments.get_by_idempotency_key(command.idempotency_key.value)
        if existing is None:
            # транзакция конкурента откатилась — конфликта на самом деле нет
            raise original from original
        return self._existing(command, existing)

    def _existing(self, command: CreatePaymentCommand, existing: Payment) -> CreatePaymentResult:
        if existing.fingerprint != self._fingerprint(command):
            raise IdempotencyConflict(command.idempotency_key.value)
        return CreatePaymentResult(payment=existing, created=False)
