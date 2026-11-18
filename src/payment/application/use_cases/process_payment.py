"""Use case: обработка платежа (consumer)."""

import asyncio
from typing import Callable, Optional

from payment.application.dto import ProcessPaymentResult
from payment.application.exceptions import PaymentNotFound
from payment.application.ports.payment_gateway import GatewayResult, PaymentGateway
from payment.application.ports.repositories import AbstractUnitOfWork
from payment.application.ports.webhook_sender import WebhookSender
from payment.domain.model.enums import PaymentStatus
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy


class ProcessPayment:
    """Обрабатывает один платёж: шлюз -> статус в БД -> webhook.

    Статус коммитится ДО отправки webhook: недоступный webhook не должен
    возвращать платёж в очередь. Webhook шлём с ретраями, но их исход
    на статус не влияет (failed webhook = потеря уведомления, не потеря платежа).
    """

    def __init__(
        self,
        *,
        uow: AbstractUnitOfWork,
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
        retry_policy: Optional[ExponentialBackoffRetryPolicy] = None,
        sign: Optional[Callable[[dict], str]] = None,
    ) -> None:
        self._uow = uow
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._retry_policy = retry_policy or ExponentialBackoffRetryPolicy()
        self._sign = sign or (lambda payload: "")

    async def execute(self, payment_id: PaymentId) -> ProcessPaymentResult:
        async with self._uow as uow:
            payment = await uow.payments.get(payment_id)
            if payment is None:
                raise PaymentNotFound(str(payment_id))
            if payment.status is not PaymentStatus.PENDING:
                # at-least-once: дубликат сообщения по уже обработанному платежу
                return ProcessPaymentResult(
                    payment_id=payment.id.value,
                    status=payment.status.value,
                    failure_reason=payment.failure_reason,
                )

            result: GatewayResult = await self._gateway.process(
                payment_id=str(payment.id),
                amount=payment.amount.amount,
                currency=payment.amount.currency.value,
            )
            payment.mark_succeeded() if result.succeeded else payment.mark_failed(
                reason=result.reason or "gateway rejected the payment"
            )
            # репозиторий возвращает detached-объекты: UPDATE делаем явно через save()
            await uow.payments.save(payment)
            await uow.commit()
            payment.collect_events()

        await self._notify(payment)
        return ProcessPaymentResult(
            payment_id=payment.id.value,
            status=payment.status.value,
            failure_reason=payment.failure_reason,
        )

    async def _notify(self, payment: Payment) -> None:
        payload = payment.to_public_dict()
        signature = self._sign(payload)
        attempts = 0
        while True:
            attempts += 1
            try:
                await self._webhook_sender.send(
                    url=payment.webhook_url.value, payload=payload, signature=signature
                )
                return
            except Exception:
                if self._retry_policy.is_exhausted(attempts=attempts):
                    return  # уведомление потеряно; статус платежа уже закоммичен
                # экспоненциальная задержка перед следующей попыткой
                await asyncio.sleep(self._retry_policy.delay_for_attempt(attempts))
