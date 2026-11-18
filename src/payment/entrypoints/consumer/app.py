"""FastStream-приложение consumer'а: очередь из PAYMENT_RABBITMQ_QUEUE.

Один обработчик делает всё: получает сообщение, обрабатывает платёж,
обновляет статус и отправляет webhook. Инфраструктурные сбои (БД и т.п.)
уходят в retry-очереди с экспоненциальным TTL, после max_attempts — в DLQ.
Счётчик попыток едет в заголовке `x-attempt` (x-death теряется при
повторной публикации).
"""

import asyncio
import json
import logging
import signal

from faststream.rabbit import RabbitBroker, RabbitMessage

from payment.adapters.messaging.connection import connect_with_retry
from payment.adapters.messaging.routing import ATTEMPT_HEADER, route_failure
from payment.adapters.messaging.schemas import PaymentCreatedMessage
from payment.adapters.messaging.topology import declare_topology, main_queue
from payment.application.use_cases.process_payment import ProcessPayment
from payment.bootstrap import Container
from payment.domain.model.value_objects import PaymentId

logger = logging.getLogger(__name__)


async def _ack(message: RabbitMessage) -> None:
    try:
        await message.ack()
    except Exception:  # noqa: S110 — ack может упасть при разрыве соединения
        logger.debug("ack failed", exc_info=True)


def build_consumer_app(container: Container | None = None) -> RabbitBroker:
    container = container or Container()
    broker = container.get_broker()
    policy = container.retry_policy
    queue_name = container.settings.rabbitmq_queue
    dlq_name = container.settings.rabbitmq_dlq
    prefetch = container.settings.consumer_prefetch

    @broker.subscriber(main_queue(queue_name, dlq_name), consume_args={"prefetch_count": prefetch})
    async def handle_payment_created(message: RabbitMessage) -> None:
        use_case: ProcessPayment = container.process_payment()
        payload = message.body
        if isinstance(payload, bytes):
            payload = json.loads(payload)
        try:
            event = PaymentCreatedMessage.model_validate(payload)
        except ValueError:
            # не наше сообщение — в DLQ, ретраить бессмысленно
            logger.error("poison message -> %s: %r", dlq_name, payload)
            await broker.publish(payload, queue=dlq_name)
            await _ack(message)
            return
        try:
            result = await use_case.execute(PaymentId.from_string(event.payment_id))
            logger.info("payment %s processed: %s", event.payment_id, result.status)
        except Exception as exc:  # noqa: BLE001 — инфраструктурный сбой: retry/DLQ
            decision = route_failure(
                message.headers, policy, queue_name=queue_name, dlq_name=dlq_name
            )
            if decision.action == "retry":
                logger.warning(
                    "payment %s failed (%s); retry %d via %s",
                    event.payment_id,
                    exc,
                    decision.attempt,
                    decision.queue,
                )
                await broker.publish(
                    payload, queue=decision.queue, headers={ATTEMPT_HEADER: decision.attempt}
                )
            else:
                logger.error(
                    "payment %s failed %d times (%s); -> %s",
                    event.payment_id,
                    decision.attempt,
                    exc,
                    decision.queue,
                )
                await broker.publish(payload, queue=decision.queue)
            await _ack(message)

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
