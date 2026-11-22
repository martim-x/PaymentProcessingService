"""Перепубликация упавших сообщений (retry/DLQ) без потери при сбое publish.

Порядок важен: сначала публикуем копию в retry/DLQ, и только после успешного
подтверждения ack'аем исходное сообщение. Если публикация не удалась (брокер
отклонил, очередь удалена, канал закрылся), исходное сообщение возвращается в
очередь (`nack requeue`), а не ack'ается — иначе оно было бы потеряно.
"""

import logging
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitMessage

from payment.adapters.messaging.publisher import FastStreamPublisher
from payment.adapters.messaging.routing import (
    BrokerRetryHeader,
    FailureRoute,
)

logger = logging.getLogger(__name__)


class MessageRedeliverer:
    """Publishes retry/DLQ copies and safely settles their source messages."""

    @staticmethod
    async def ack_safely(*, message: RabbitMessage) -> None:
        try:
            await message.ack()
        except Exception:  # noqa: S110 — ack может упасть при разрыве соединения
            logger.debug("ack failed", exc_info=True)

    @staticmethod
    async def requeue_safely(*, message: RabbitMessage) -> None:
        try:
            await message.nack(requeue=True)
        except Exception:  # noqa: S110 — соединение уже потеряно: брокер вернёт сам
            logger.debug("nack(requeue) failed", exc_info=True)

    @staticmethod
    async def publish_or_requeue(
        *,
        broker: RabbitBroker,
        message: RabbitMessage,
        payload: Any,
        queue: str,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        """Republish then ACK; on failure requeue the source message."""
        try:
            confirmation = await broker.publish(
                payload,
                queue=queue,
                persist=True,
                mandatory=True,
                headers=headers,
            )
            FastStreamPublisher.ensure_confirmed(
                confirmation=confirmation,
                subject=queue,
            )
        except Exception:  # noqa: BLE001 — сообщение нельзя терять из-за сбоя publish
            logger.exception(
                "publish to %s failed; requeueing the original message", queue
            )
            await MessageRedeliverer.requeue_safely(message=message)
            return False
        await MessageRedeliverer.ack_safely(message=message)
        return True

    @staticmethod
    async def publish_failure(
        *,
        broker: RabbitBroker,
        message: RabbitMessage,
        payload: Any,
        decision: FailureRoute,
    ) -> bool:
        """Forward the retry counters selected by the failure route."""
        headers = None
        if decision.action == "retry":
            headers = {
                header.value: message.headers[header.value]
                for header in (
                    BrokerRetryHeader.ATTEMPT,
                    BrokerRetryHeader.INFRA_ATTEMPT,
                )
                if header.value in (message.headers or {})
            }
            headers[decision.header.value] = decision.header_value
        return await MessageRedeliverer.publish_or_requeue(
            broker=broker,
            message=message,
            payload=payload,
            queue=decision.queue,
            headers=headers,
        )
