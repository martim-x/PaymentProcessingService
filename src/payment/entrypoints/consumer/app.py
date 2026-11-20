"""FastStream-приложение consumer'а: очередь из PAYMENT_RABBITMQ_QUEUE.

- Один обработчик обрабатывает платеж и отправляет webhook до ACK; повторная
  доставка после ошибки webhook возобновляет финализированный платёж без повторного
  вызова шлюза.

Отказы делятся на два класса (см. `adapters.messaging.error_classification`):
- транзиентный сбой БД/транспорта (`OperationalError`, разрыв соединения с
  брокером) — отдельная лестница задержек `payments.new.infra.retry.N`
  (`x-infra-attempt`, `PAYMENT_INFRA_RETRY_BASE_DELAY`): сбой инфраструктуры
  получает более щедрые паузы;
- остальные ошибки — обычная попытка обработки: `x-attempt`, экспоненциальный TTL.

Бюджет отказов у классов общий: предел `PAYMENT_MAX_ATTEMPTS` считается по сумме
`x-attempt` и `x-infra-attempt`, поэтому сообщение попадает в DLQ ровно после
третьего отказа независимо от их чередования. `PaymentBusy` (клейм удерживается
другим обработчиком) откладывает сообщение, не расходуя бюджет.

Счётчики едут в заголовках (`x-attempt`/`x-infra-attempt`): x-death теряется при
повторной публикации. Если публикация retry/DLQ не удалась, исходное сообщение
возвращается в очередь, а не ack'ается.
"""

import asyncio
import json
import logging
import signal
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitMessage

from payment.adapters.messaging.connection import connect_with_retry
from payment.adapters.messaging.error_classification import is_transient_infra_error
from payment.adapters.messaging.redelivery import (
    ack_safely,
    publish_failure,
    publish_or_requeue,
)
from payment.adapters.messaging.routing import route_busy, route_failure, route_infra_failure
from payment.adapters.messaging.schemas import PaymentCreatedMessage
from payment.adapters.messaging.topology import declare_topology, main_queue
from payment.application.exceptions import PaymentBusy, WebhookBusy, WebhookDeliveryExhausted
from payment.application.services.payment_service import PaymentService
from payment.bootstrap import Container
from payment.domain.model.value_objects import PaymentId

logger = logging.getLogger(__name__)


async def parse_or_quarantine_message(
    message: RabbitMessage, broker: RabbitBroker, dlq_name: str
) -> tuple[PaymentCreatedMessage, PaymentId, Any] | None:
    """Parse a payment event; send malformed or invalid IDs to the DLQ."""
    raw_payload = message.body
    try:
        payload = json.loads(raw_payload) if isinstance(raw_payload, bytes) else raw_payload
        event = PaymentCreatedMessage.model_validate(payload)
        payment_id = PaymentId.from_string(event.payment_id)
    except (TypeError, ValueError):
        logger.error("poison message -> %s: %r", dlq_name, raw_payload)
        await publish_or_requeue(
            broker=broker, message=message, payload=raw_payload, queue=dlq_name
        )
        return None
    return event, payment_id, payload


async def handle_payment_created(
    message: RabbitMessage, *, broker: RabbitBroker, container: Container
) -> None:
    """Обработчик одного события: обработка платежа -> ack | retry | DLQ."""
    settings = container.settings
    queue_name = settings.rabbitmq_queue
    dlq_name = settings.rabbitmq_dlq
    infra_policy = settings.infra_retry_policy

    parsed = await parse_or_quarantine_message(message, broker, dlq_name)
    if parsed is None:
        return
    event, payment_id, payload = parsed
    payment_service: PaymentService = container.payment_service()
    try:
        result = await payment_service.process_payment(payment_id)
        if result.webhook_delivery is None:
            raise RuntimeError(
                f"payment {event.payment_id} has no recoverable webhook delivery payload"
            )
        delivery_result = await container.webhook_service().deliver(result.webhook_delivery)
        if delivery_result.stale:
            raise WebhookBusy(event.payment_id)
    except WebhookDeliveryExhausted as exc:
        logger.error("payment %s webhook exhausted: %s -> %s", event.payment_id, exc, dlq_name)
        await publish_or_requeue(broker=broker, message=message, payload=payload, queue=dlq_name)
        return
    except (PaymentBusy, WebhookBusy) as exc:
        decision = route_busy(message.headers, container.retry_policy, queue_name=queue_name)
        logger.warning(
            "payment %s: %s; retry via %s without spending an attempt",
            event.payment_id,
            exc,
            decision.queue,
        )
        await publish_failure(broker, message, payload, decision)
        return
    except Exception as exc:  # noqa: BLE001 — сбой обработки: retry/DLQ
        infra = is_transient_infra_error(exc)
        decision = (
            route_infra_failure(
                message.headers, infra_policy, queue_name=queue_name, dlq_name=dlq_name
            )
            if infra
            else route_failure(
                message.headers, container.retry_policy, queue_name=queue_name, dlq_name=dlq_name
            )
        )
        label = "infrastructure failure" if infra else "failure"
        if decision.action == "retry":
            logger.warning(
                "payment %s %s (%s); retry %d via %s",
                event.payment_id,
                label,
                exc,
                decision.attempt,
                decision.queue,
            )
        else:
            logger.error(
                "payment %s %s after %d attempts (%s); -> %s (payment stays unresolved)",
                event.payment_id,
                label,
                decision.attempt,
                exc,
                decision.queue,
            )
        await publish_failure(broker, message, payload, decision)
        return

    logger.info("payment %s processed: %s", event.payment_id, result.status)
    await ack_safely(message)


def build_consumer_app(container: Container | None = None) -> RabbitBroker:
    container = container or Container()
    broker = container.get_broker()
    queue_name = container.settings.rabbitmq_queue
    dlq_name = container.settings.rabbitmq_dlq

    # prefetch задан на канале брокера (Container.make_channel), не здесь
    @broker.subscriber(main_queue(queue_name, dlq_name))
    async def handle(message: RabbitMessage) -> None:
        await handle_payment_created(message, broker=broker, container=container)

    return broker


async def run_consumer(container: Container | None = None) -> None:
    """Точка входа consumer-процесса (retry подключения к брокеру при старте)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    container = container or Container()
    broker = build_consumer_app(container)
    await connect_with_retry(broker)
    await declare_topology(
        broker,
        container.retry_policy,
        container.settings.rabbitmq_queue,
        container.settings.rabbitmq_dlq,
        infra_policy=container.settings.infra_retry_policy,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await broker.start()  # объявляет подписки; start() не блокирует
        await stop.wait()  # живём, пока не придёт сигнал
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(run_consumer())
