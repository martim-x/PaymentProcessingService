"""Use case: обработка платежа (consumer)."""

import asyncio
import logging

from payment.application.dto import ProcessPaymentResult
from payment.application.exceptions import PaymentBusy, PaymentNotFound
from payment.application.ports.payment_gateway import GatewayResult, PaymentGateway
from payment.application.ports.repositories import AbstractUnitOfWork
from payment.application.ports.webhook_sender import WebhookSender
from payment.domain.model.enums import PaymentStatus
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

logger = logging.getLogger(__name__)


class ProcessPayment:
    """Обрабатывает один платёж: claim -> шлюз -> финальный статус -> webhook.

    - заявка на обработку берётся ДО вызова шлюза: атомарный
      `UPDATE ... SET locked_until = now() + ttl WHERE status='pending' AND
      (locked_until IS NULL OR locked_until < now())`. Дубликат сообщения
      клейм не получит, поэтому списание не повторится;
    - при любом сбое после клейма он СНИМАЕТСЯ (`release_claim`), иначе
      ретраи (TTL 1–2 с) истекут раньше клейма (60 с) и платёж зависнет в
      `pending` без единой попытки — это и есть главный failure mode;
    - если клейм занят другим consumer'ом, а платёж ещё `pending` — бросаем
      `PaymentBusy`: сообщение идёт по retry/DLQ, а не ack'ается молча;
    - в шлюз передаётся idempotency_key (id платежа) — второй рубеж защиты;
    - шлюз вызывается ВНЕ транзакции, финальный статус фиксируется условным
      UPDATE под токеном клейма;
    - статус коммитится ДО отправки webhook.
    """

    def __init__(
        self,
        *,
        uow: AbstractUnitOfWork,
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
        retry_policy: ExponentialBackoffRetryPolicy | None = None,
        claim_ttl_seconds: float = 60.0,
    ) -> None:
        self._uow = uow
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._retry_policy = retry_policy or ExponentialBackoffRetryPolicy()
        self._claim_ttl = claim_ttl_seconds

    async def execute(self, payment_id: PaymentId) -> ProcessPaymentResult:
        claim_token = await self._claim(payment_id)
        if claim_token is None:
            payment = await self._load(payment_id)
            if payment.status is not PaymentStatus.PENDING:
                # платёж уже финализирован — дубликат сообщения можно ack'ать
                return self._result(payment)
            # платёж ещё pending, но клейм у другого consumer'а:
            # молча ack'ать нельзя — иначе при его падении не останется попыток
            raise PaymentBusy(str(payment_id))

        try:
            payment = await self._load(payment_id)
            result: GatewayResult = await self._gateway.process(
                payment_id=str(payment.id),
                amount=payment.amount.amount,
                currency=payment.amount.currency.value,
                idempotency_key=str(payment.id),
            )
            payment.mark_succeeded() if result.succeeded else payment.mark_failed(
                reason=result.reason or "gateway rejected the payment"
            )
            payment.collect_events()  # события домена не имеют подписчиков
            won = await self._finalize(payment, claim_token)
        except Exception:
            # снимаем клейм, чтобы ретрай снова смог взять платёж в работу
            await self._release_claim(payment_id, claim_token)
            raise

        if not won:
            logger.info("payment %s: claim expired or already finalized", payment.id)
            return self._result(payment)

        await self._notify(payment)
        return self._result(payment)

    async def _claim(self, payment_id: PaymentId):
        async with self._uow as uow:
            token = await uow.payments.try_claim(payment_id, self._claim_ttl)
            await uow.commit()
        return token

    async def _release_claim(self, payment_id: PaymentId, claim_token) -> None:
        try:
            async with self._uow as uow:
                await uow.payments.release_claim(payment_id, claim_token)
                await uow.commit()
        except Exception:  # noqa: BLE001 — снятие клейма не должно маскировать исходную ошибку
            logger.exception("failed to release claim for payment %s", payment_id)

    async def _load(self, payment_id: PaymentId) -> Payment:
        async with self._uow as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        return payment

    async def _finalize(self, payment: Payment, claim_token) -> bool:
        async with self._uow as uow:
            won = await uow.payments.try_finalize(payment, claim_token)
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
        attempts = 0
        while True:
            attempts += 1
            try:
                # подпись ставит адаптер: сериализация и HMAC живут в одном месте
                await self._webhook_sender.send(url=payment.webhook_url.value, payload=payload)
                return
            except Exception:  # noqa: BLE001 — недоставка фиксируется, статус уже сохранён
                if self._retry_policy.is_exhausted(attempts=attempts):
                    logger.error("webhook delivery failed for payment %s", payment.id)
                    return
                await asyncio.sleep(self._retry_policy.delay_for_attempt(attempts))
