"""Use case: обработка платежа (consumer)."""

import asyncio
import logging
from typing import Callable

from payment.application.dto import ProcessPaymentResult
from payment.application.exceptions import PaymentNotFound
from payment.application.ports.payment_gateway import GatewayResult, PaymentGateway
from payment.application.ports.repositories import AbstractUnitOfWork
from payment.application.ports.webhook_sender import WebhookSender
from payment.domain.model.enums import PaymentStatus
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

logger = logging.getLogger(__name__)


class ProcessPayment:
    """Обрабатывает один платёж: шлюз -> финальный статус -> webhook.

    - шлюз вызывается ВНЕ транзакции (не держим соединение на внешнем I/O);
    - финальный статус пишется условным UPDATE ... WHERE status='pending':
      при дубликате сообщения второй consumer получит False и не продублирует
      ни списание, ни webhook;
    - статус коммитится ДО отправки webhook: недоступный webhook не должен
      возвращать платёж в очередь.
    """

    def __init__(
        self,
        *,
        uow: AbstractUnitOfWork,
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
        retry_policy: ExponentialBackoffRetryPolicy | None = None,
        sign: Callable[[dict], str] | None = None,
    ) -> None:
        self._uow = uow
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._retry_policy = retry_policy or ExponentialBackoffRetryPolicy()
        self._sign = sign or (lambda payload: "")

    async def execute(self, payment_id: PaymentId) -> ProcessPaymentResult:
        payment = await self._load(payment_id)
        if payment.status is not PaymentStatus.PENDING:
            # at-least-once: дубликат сообщения по уже обработанному платежу
            return self._result(payment)

        result: GatewayResult = await self._gateway.process(
            payment_id=str(payment.id),
            amount=payment.amount.amount,
            currency=payment.amount.currency.value,
        )
        payment.mark_succeeded() if result.succeeded else payment.mark_failed(
            reason=result.reason or "gateway rejected the payment"
        )
        payment.collect_events()  # события домена не имеют подписчиков — просто очищаем буфер

        won = await self._finalize(payment)
        if not won:
            logger.info("payment %s already processed by another consumer", payment.id)
            return self._result(payment)

        await self._notify(payment)
        return self._result(payment)

    async def _load(self, payment_id: PaymentId) -> Payment:
        async with self._uow as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        return payment

    async def _finalize(self, payment: Payment) -> bool:
        async with self._uow as uow:
            won = await uow.payments.try_finalize(payment)
            await uow.commit()
        return won

    @staticmethod
    def _result(payment: Payment) -> ProcessPaymentResult:
        return ProcessPaymentResult(
            payment_id=payment.id.value,
            status=payment.status.value,
            failure_reason=payment.failure_reason,
        )

    def _payload(self, payment: Payment) -> dict:
        """Компактный webhook-payload: без metadata и собственного webhook_url."""
        return {
            "id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount.amount),
            "currency": payment.amount.currency.value,
            "failure_reason": payment.failure_reason,
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        }

    async def _notify(self, payment: Payment) -> None:
        payload = self._payload(payment)
        signature = self._sign(payload)
        attempts = 0
        while True:
            attempts += 1
            try:
                await self._webhook_sender.send(
                    url=payment.webhook_url.value, payload=payload, signature=signature
                )
                return
            except Exception:  # noqa: BLE001 — недоставка фиксируется, статус уже сохранён
                if self._retry_policy.is_exhausted(attempts=attempts):
                    logger.error("webhook delivery failed for payment %s", payment.id)
                    return
                await asyncio.sleep(self._retry_policy.delay_for_attempt(attempts))
