"""Сервис доставки уведомлений webhook через RabbitMQ."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

from payment.application.dto import WebhookDeliveryCommand
from payment.application.exceptions import (
    WebhookBusy,
    WebhookDeliveryExhausted,
    WebhookDeliveryFailed,
)
from payment.application.ports.repositories import (
    DELIVERY_DELIVERED,
    DELIVERY_FAILED,
    WebhookDelivery,
)
from payment.application.ports.unit_of_work import AbstractUnitOfWork
from payment.application.ports.webhook_sender import WebhookSender

logger = logging.getLogger(__name__)


@dataclass(
    frozen=True,
    slots=True,
    kw_only=True,
)
class WebhookDeliveryResult:
    """Состояние доставки webhook после обработки события."""

    event_id: str
    status: str
    already_delivered: bool = False
    stale: bool = False


class WebhookService:
    """Отправляет webhook и фиксирует состояние задачи под fencing token."""

    def __init__(
        self,
        *,
        uow: AbstractUnitOfWork,
        sender: WebhookSender,
        claim_ttl_seconds: float = 60.0,
        max_attempts: int = 3,
    ) -> None:
        self._uow = uow
        self._sender = sender
        self._claim_ttl = claim_ttl_seconds
        self._max_attempts = max_attempts

    async def deliver(
        self,
        *,
        command: WebhookDeliveryCommand,
    ) -> WebhookDeliveryResult:
        """Доставляет один event_id; шлюз платежей в этом сервисе не вызывается."""
        claim_token = await self._open_task(command=command)
        if claim_token is None:
            return await self._result_without_claim(command=command)

        try:
            await self._sender.send(
                url=command.url,
                payload=command.body,
            )
        except asyncio.CancelledError:
            await self._release_claim(
                event_id=command.event_id,
                claim_token=claim_token,
            )
            raise
        except (
            Exception
        ) as exc:  # noqa: BLE001 — сбой отправки пойдёт в RabbitMQ retry/DLQ
            outcome = await self._mark_failed(
                event_id=command.event_id,
                claim_token=claim_token,
                exc=exc,
            )
            if outcome is None:
                raise WebhookBusy(event_id=command.event_id) from exc
            attempts, exhausted = outcome
            if exhausted:
                raise WebhookDeliveryExhausted(
                    event_id=command.event_id,
                    attempts=attempts,
                    error=str(exc),
                ) from exc
            raise WebhookDeliveryFailed(
                event_id=command.event_id,
                attempt=attempts,
                error=str(exc),
            ) from exc

        if not await self._mark_delivered(
            event_id=command.event_id,
            claim_token=claim_token,
        ):
            logger.warning(
                "webhook %s sent with a stale claim",
                command.event_id,
            )
            return WebhookDeliveryResult(
                event_id=command.event_id,
                status="delivering",
                stale=True,
            )
        return WebhookDeliveryResult(
            event_id=command.event_id,
            status=DELIVERY_DELIVERED,
        )

    async def _result_without_claim(
        self,
        *,
        command: WebhookDeliveryCommand,
    ) -> WebhookDeliveryResult:
        record = await self._get(event_id=command.event_id)
        if record is not None and record.status == DELIVERY_DELIVERED:
            return WebhookDeliveryResult(
                event_id=command.event_id,
                status=record.status,
                already_delivered=True,
            )
        if record is not None and record.status == DELIVERY_FAILED:
            raise WebhookDeliveryExhausted(
                event_id=command.event_id,
                attempts=record.attempts,
                error=record.last_error or "delivery failed",
            )
        raise WebhookBusy(event_id=command.event_id)

    async def _open_task(
        self,
        *,
        command: WebhookDeliveryCommand,
    ) -> datetime | None:
        async with self._uow as uow:
            await uow.webhook_deliveries.create_if_absent(
                event_id=command.event_id,
                payment_id=command.payment_id,
            )
            token = await uow.webhook_deliveries.try_claim(
                event_id=command.event_id,
                ttl_seconds=self._claim_ttl,
            )
            await uow.commit()
        return token

    async def _get(
        self,
        *,
        event_id: str,
    ) -> WebhookDelivery | None:
        async with self._uow as uow:
            return await uow.webhook_deliveries.get(
                event_id=event_id,
            )

    async def _release_claim(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> None:
        try:
            await asyncio.shield(
                self._do_release_claim(
                    event_id=event_id,
                    claim_token=claim_token,
                )
            )
        except BaseException:  # noqa: BLE001 — не маскируем исходную отмену
            logger.exception(
                "failed to release webhook claim for %s",
                event_id,
            )

    async def _do_release_claim(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> None:
        async with self._uow as uow:
            await uow.webhook_deliveries.release_claim(
                event_id=event_id,
                claim_token=claim_token,
            )
            await uow.commit()

    async def _mark_delivered(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> bool:
        async with self._uow as uow:
            recorded = await uow.webhook_deliveries.mark_delivered(
                event_id=event_id,
                claim_token=claim_token,
            )
            await uow.commit()
        return recorded

    async def _mark_failed(
        self,
        *,
        event_id: str,
        claim_token: datetime,
        exc: BaseException,
    ) -> tuple[int, bool] | None:
        async with self._uow as uow:
            outcome = await uow.webhook_deliveries.mark_attempt_failed(
                event_id=event_id,
                claim_token=claim_token,
                max_attempts=self._max_attempts,
                error=str(exc),
            )
            await uow.commit()
        return outcome
