"""Перепубликация упавших сообщений (retry/DLQ) без потери при сбое publish.

Порядок важен: сначала публикуем копию в retry/DLQ, и только после успешного
подтверждения ack'аем исходное сообщение. Если публикация не удалась (брокер
отклонил, очередь удалена, канал закрылся), исходное сообщение возвращается в
очередь (`nack requeue`), а не ack'ается — иначе оно было бы потеряно.
"""

import logging
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitMessage

from payment.adapters.messaging.publisher import ensure_confirmed
from payment.adapters.messaging.routing import (
    ATTEMPT_HEADER,
    INFRA_ATTEMPT_HEADER,
    FailureRoute,
)

logger = logging.getLogger(__name__)


async def ack_safely(message: RabbitMessage) -> None:
    try:
        await message.ack()
    except Exception:  # noqa: S110 — ack может упасть при разрыве соединения
        logger.debug(
            "ack failed",
            exc_info=True,
        )


async def requeue_safely(message: RabbitMessage) -> None:
    try:
        await message.nack(requeue=True)
    except Exception:  # noqa: S110 — соединение уже потеряно: брокер вернёт сам
        logger.debug(
            "nack(requeue) failed",
            exc_info=True,
        )


async def publish_or_requeue(
    *,
    broker: RabbitBroker,
    message: RabbitMessage,
    payload: Any,
    queue: str,
    headers: dict[str, Any] | None = None,
) -> bool:
    """Публикует копию и ack'ает исходное сообщение; при сбое — requeue.

    True — сообщение переиздано и подтверждено; False — публикация не удалась,
    исходное сообщение возвращено в очередь (не потеряно).
    """
    try:
        confirmation = await broker.publish(
            payload,
            queue=queue,
            persist=True,
            mandatory=True,
            headers=headers,
        )
        ensure_confirmed(
            confirmation,
            queue,
        )
    except Exception:  # noqa: BLE001 — сообщение нельзя терять из-за сбоя publish
        logger.exception(
            "publish to %s failed; requeueing the original message",
            queue,
        )
        await requeue_safely(message)
        return False
    await ack_safely(message)
    return True


async def publish_failure(
    broker: RabbitBroker,
    message: RabbitMessage,
    payload: Any,
    decision: FailureRoute,
) -> bool:
    """Маршрут отказа: retry-очередь несёт свой счётчик, DLQ — без счётчика.

    Заголовок берётся из решения (`x-attempt` для бизнес-отказа,
    `x-infra-attempt` для транзиентного сбоя БД/транспорта): каждый счётчик
    задаёт ступень задержки в своей лестнице, а общий бюджет считается по их
    сумме. Оба счётчика переносятся, чтобы сумма не потерялась.
    """
    headers = None
    if decision.action == "retry":
        headers = {
            header: message.headers[header]
            for header in (
                ATTEMPT_HEADER,
                INFRA_ATTEMPT_HEADER,
            )
            if header in (message.headers or {})
        }
        headers[decision.header] = decision.header_value
    return await publish_or_requeue(
        broker=broker,
        message=message,
        payload=payload,
        queue=decision.queue,
        headers=headers,
    )
