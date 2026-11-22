"""Сервис платежей: создание, чтение и асинхронная обработка.

Сервис координирует сценарии и обращается к репозиториям через Unit of Work.
Правила значений и переходов статуса остаются в доменной модели `Payment`.
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, cast

from payment.application.dto import (
    CreatePaymentCommand,
    CreatePaymentResult,
    PaymentDetails,
    ProcessPaymentResult,
    WebhookDeliveryCommand,
)
from payment.application.event_types import OutboxEventType
from payment.application.exceptions import (
    IdempotencyConflict,
    IdempotencyViolation,
    PaymentBusy,
    PaymentNotFound,
)
from payment.application.ports.payment_gateway import GatewayResult, PaymentGateway
from payment.application.ports.repositories import OutboxDispatch
from payment.application.ports.unit_of_work import AbstractUnitOfWork
from payment.domain.events import PaymentCreated
from payment.domain.model.enums import PaymentStatus
from payment.domain.model.fingerprint import PaymentFingerprint
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import Money, PaymentId, WebhookUrl

logger = logging.getLogger(__name__)


class PaymentService:
    """Операции над платежом: create/get/process.

    Сервис координирует шаги через UoW и шлюз; инварианты и переходы статуса
    проверяются агрегатом `Payment`. HTTP- и RabbitMQ-форматы остаются во входах.
    Экземпляр имеет собственный UoW и создаётся на запрос или сообщение.
    """

    def __init__(
        self,
        *,
        uow: AbstractUnitOfWork,
        gateway: PaymentGateway,
        claim_ttl_seconds: float = 60.0,
        claim_wait_seconds: float = 15.0,
        claim_poll_seconds: float = 0.5,
    ) -> None:
        self._uow = uow
        self._gateway = gateway
        self._claim_ttl = claim_ttl_seconds
        self._claim_wait = claim_wait_seconds
        self._claim_poll = claim_poll_seconds

    @staticmethod
    def _fingerprint(*, command: CreatePaymentCommand) -> str:
        return PaymentFingerprint.compute(
            payload={
                "amount": PaymentFingerprint.canonical_amount(amount=command.amount),
                "currency": command.currency.value,
                "description": command.description,
                "metadata": dict(command.metadata),
                "webhook_url": command.webhook_url,
            }
        )

    @staticmethod
    def _outbox_payload(*, event: PaymentCreated) -> dict[str, object]:
        return {
            "event": OutboxEventType.PAYMENT_CREATED.value,
            "payment_id": str(event.payment_id),
        }

    @staticmethod
    def _build_webhook_body(*, payment: Payment, event_id: str) -> dict[str, Any]:
        return {
            "id": str(payment.id),
            "event": f"payment.{payment.status.value}",
            "event_id": event_id,
            "status": payment.status.value,
            "amount": str(payment.amount.amount),
            "currency": payment.amount.currency.value,
            "failure_reason": payment.failure_reason,
            "processed_at": (
                payment.processed_at.isoformat() if payment.processed_at else None
            ),
        }

    def _build_webhook_event(
        self, *, payment: Payment, event_id: str
    ) -> dict[str, Any]:
        return {
            "event": OutboxEventType.PAYMENT_WEBHOOK.value,
            "event_id": event_id,
            "payment_id": str(payment.id),
            "webhook_url": payment.webhook_url.value,
            "body": self._build_webhook_body(payment=payment, event_id=event_id),
        }

    async def create_payment(
        self,
        *,
        command: CreatePaymentCommand,
    ) -> CreatePaymentResult:
        """Создаёт платёж и outbox-событие атомарно; повтор ключа идемпотентен."""
        try:
            return await self._create_payment(command=command)
        except IdempotencyViolation as exc:
            logger.info(
                "idempotency race for key %r; re-reading payment",
                command.idempotency_key.value,
            )
            return await self._read_existing(
                command=command,
                original=exc,
            )

    async def _create_payment(
        self,
        *,
        command: CreatePaymentCommand,
    ) -> CreatePaymentResult:
        async with self._uow as uow:
            existing = await uow.payments.get_by_idempotency_key(
                key=command.idempotency_key.value
            )
            if existing is not None:
                return self._existing(
                    command=command,
                    existing=existing,
                )

            payment = Payment.create(
                payment_id=PaymentId.next(),
                amount=Money(
                    amount=command.amount,
                    currency=command.currency,
                ),
                description=command.description,
                metadata=command.metadata,
                idempotency_key=command.idempotency_key,
                webhook_url=WebhookUrl(value=command.webhook_url),
                fingerprint=self._fingerprint(command=command),
            )
            await uow.payments.add(payment=payment)
            for event in payment.collect_events():
                if isinstance(
                    event,
                    PaymentCreated,
                ):
                    await uow.outbox.add(
                        payload=self._outbox_payload(event=event),
                        aggregate_id=str(payment.id),
                    )
            await uow.commit()
            return CreatePaymentResult(
                payment=payment,
                created=True,
            )

    async def _read_existing(
        self,
        *,
        command: CreatePaymentCommand,
        original: IdempotencyViolation,
    ) -> CreatePaymentResult:
        async with self._uow as uow:
            existing = await uow.payments.get_by_idempotency_key(
                key=command.idempotency_key.value
            )
        if existing is None:
            raise original from original
        return self._existing(
            command=command,
            existing=existing,
        )

    def _existing(
        self,
        *,
        command: CreatePaymentCommand,
        existing: Payment,
    ) -> CreatePaymentResult:
        if existing.fingerprint != self._fingerprint(command=command):
            raise IdempotencyConflict(idempotency_key=command.idempotency_key.value)
        return CreatePaymentResult(
            payment=existing,
            created=False,
        )

    async def get_payment(
        self,
        *,
        payment_id: PaymentId,
    ) -> PaymentDetails:
        """Читает платёж; отсутствующий идентификатор даёт PaymentNotFound."""
        payment = await self._load(payment_id=payment_id)
        return PaymentDetails.from_payment(payment=payment)

    async def process_payment(
        self,
        *,
        payment_id: PaymentId,
    ) -> ProcessPaymentResult:
        """Обрабатывает платёж и атомарно записывает финальный статус с webhook-outbox.

        Claim защищает от конкурирующего вызова шлюза. Сетевой вызов выполняется
        вне транзакции; если claim потерян, перед результатом читается состояние БД.
        """
        claim_token = await self._acquire_claim(payment_id=payment_id)
        if claim_token is None:
            payment = await self._load(payment_id=payment_id)
            if payment.status is not PaymentStatus.PENDING:
                return await self._resume_result(payment=payment)
            raise PaymentBusy(payment_id=str(payment_id))

        try:
            payment = await self._load(payment_id=payment_id)
            result: GatewayResult = await self._gateway.process(
                payment_id=str(payment.id),
                amount=payment.amount.amount,
                currency=payment.amount.currency.value,
                idempotency_key=str(payment.id),
            )
            if result.succeeded:
                payment.mark_succeeded()
            else:
                payment.mark_failed(
                    reason=result.reason or "gateway rejected the payment"
                )
            event_id = uuid.uuid4().hex
            won = await self._finalize(
                payment=payment,
                claim_token=claim_token,
                event_id=event_id,
            )
        except BaseException:
            await self._release_claim(
                payment_id=payment_id,
                claim_token=claim_token,
            )
            raise

        if not won:
            actual = await self._load(payment_id=payment_id)
            if actual.status is PaymentStatus.PENDING:
                logger.warning(
                    "payment %s: claim lost while still pending; retrying",
                    payment.id,
                )
                raise PaymentBusy(payment_id=str(payment_id))
            return await self._resume_result(payment=actual)
        return self._result(
            payment=payment,
            event=self._build_webhook_event(payment=payment, event_id=event_id),
        )

    async def _acquire_claim(
        self,
        *,
        payment_id: PaymentId,
    ) -> datetime | None:
        token = await self._claim(payment_id=payment_id)
        if token is not None:
            return token
        deadline = asyncio.get_running_loop().time() + self._claim_wait
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(self._claim_poll)
            token = await self._claim(payment_id=payment_id)
            if token is not None:
                return token
            payment = await self._load(payment_id=payment_id)
            if payment.status is not PaymentStatus.PENDING:
                return None
        return None

    async def _claim(
        self,
        *,
        payment_id: PaymentId,
    ) -> datetime | None:
        async with self._uow as uow:
            token = await uow.payments.try_claim(
                payment_id=payment_id,
                ttl_seconds=self._claim_ttl,
            )
            await uow.commit()
        return token

    async def _release_claim(
        self,
        *,
        payment_id: PaymentId,
        claim_token: datetime,
    ) -> None:
        try:
            await asyncio.shield(
                self._do_release_claim(
                    payment_id=payment_id,
                    claim_token=claim_token,
                )
            )
        except BaseException:  # noqa: BLE001 — не маскируем исходную ошибку/отмену
            logger.exception(
                "failed to release claim for payment %s",
                payment_id,
            )

    async def _do_release_claim(
        self,
        *,
        payment_id: PaymentId,
        claim_token: datetime,
    ) -> None:
        async with self._uow as uow:
            await uow.payments.release_claim(
                payment_id=payment_id,
                claim_token=claim_token,
            )
            await uow.commit()

    async def _load(
        self,
        *,
        payment_id: PaymentId,
    ) -> Payment:
        async with self._uow as uow:
            payment = await uow.payments.get(payment_id=payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id=str(payment_id))
        return payment

    async def _finalize(
        self,
        *,
        payment: Payment,
        claim_token: datetime,
        event_id: str,
    ) -> bool:
        """Коммитит статус и outbox в одной транзакции."""
        async with self._uow as uow:
            won = await uow.payments.try_finalize(
                payment=payment,
                claim_token=claim_token,
            )
            if won:
                # Webhook доставляется самим consumer'ом платежа. Строка нужна
                # для стабильного event_id и возобновления после падения, но
                # публиковать её нельзя: dispatch='local' прямо говорит, что
                # relay эту запись не читает, а sent остаётся false.
                await uow.outbox.add(
                    payload=self._build_webhook_event(
                        payment=payment, event_id=event_id
                    ),
                    aggregate_id=str(payment.id),
                    dispatch=OutboxDispatch.LOCAL,
                )
            await uow.commit()
        return won

    async def _resume_result(
        self,
        *,
        payment: Payment,
    ) -> ProcessPaymentResult:
        async with self._uow as uow:
            record = await uow.outbox.find_event(
                aggregate_id=str(payment.id),
                event_type=OutboxEventType.PAYMENT_WEBHOOK.value,
            )
        return self._result(
            payment=payment,
            event=record.payload if record is not None else None,
        )

    @staticmethod
    def _result(
        *,
        payment: Payment,
        event: dict[str, Any] | None,
    ) -> ProcessPaymentResult:
        command = None
        if event is not None:
            event_id = event.get("event_id")
            body = event.get("body")
            url = event.get("webhook_url")
            if (
                isinstance(
                    event_id,
                    str,
                )
                and isinstance(
                    body,
                    dict,
                )
                and all(
                    isinstance(
                        key,
                        str,
                    )
                    for key in cast(
                        dict[Any, Any],
                        body,
                    )
                )
                and isinstance(
                    url,
                    str,
                )
            ):
                typed_body = cast(
                    dict[str, Any],
                    body,
                )
                command = WebhookDeliveryCommand(
                    event_id=event_id,
                    payment_id=payment.id.value,
                    url=url,
                    body=typed_body,
                )
        return ProcessPaymentResult(
            payment_id=payment.id.value,
            status=payment.status.value,
            failure_reason=payment.failure_reason,
            webhook_delivery=command,
        )
