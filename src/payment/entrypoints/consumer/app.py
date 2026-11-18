"""FastStream-приложение consumer'а: очередь payments.new.

Один обработчик делает всё: получает сообщение, обрабатывает платёж,
обновляет статус и отправляет webhook. Инфраструктурные сбои (БД и т.п.)
уходят в retry-очереди с экспоненциальным TTL, после max_attempts — в DLQ.
Счётчик попыток едет в заголовке `x-attempt`, потому что x-death теряется
при повторной публикации.
"""

import asyncio
import json
import logging
import signal

from faststream.rabbit import RabbitBroker, RabbitMessage

from payment.adapters.messaging.connection import connect_with_retry
from payment.adapters.messaging.schemas import PaymentCreatedMessage
from payment.adapters.messaging.topology import (
    DLQ_QUEUE,
    RETRY_QUEUE_FMT,
    declare_topology,
    main_queue,
)
from payment.application.use_cases.process_payment import ProcessPayment
from payment.bootstrap import Container
from payment.entrypoints.consumer.handlers import ATTEMPT_HEADER, next_attempt, parse_payment_id

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

    @broker.subscriber(main_queue())
    async def handle_payment_created(message: RabbitMessage) -> None:
        use_case: ProcessPayment = container.process_payment()
        payload = message.body
        if isinstance(payload, bytes):
            payload = json.loads(payload)
        try:
            event = PaymentCreatedMessage.model_validate(payload)
        except ValueError:
            # не наше сообщение — в DLQ, ретраить бессмысленно
            await broker.publish(payload, queue=DLQ_QUEUE)
            await _ack(message)
            return
        try:
            result = await use_case.execute(parse_payment_id(event.model_dump()))
            logger.info("payment %s processed: %s", event.payment_id, result.status)
        except Exception as exc:  # noqa: BLE001 — инфраструктурный сбой: retry/DLQ
            attempt = next_attempt(message.headers)
            if not policy.is_exhausted(attempts=attempt):
                target = RETRY_QUEUE_FMT.format(attempt=attempt)
                logger.warning(
                    "payment %s failed (%s); retry %d via %s",
                    event.payment_id,
                    exc,
                    attempt,
                    target,
                )
                await broker.publish(payload, queue=target, headers={ATTEMPT_HEADER: attempt})
            else:
                logger.error(
                    "payment %s failed %d times (%s); -> DLQ", event.payment_id, attempt, exc
                )
                await broker.publish(payload, queue=DLQ_QUEUE)
            await _ack(message)

    return broker


async def run_consumer(container: Container | None = None) -> None:
    """Точка входа consumer-процесса (retry подключения к брокеру при старте)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    container = container or Container()
    broker = build_consumer_app(container)
    await connect_with_retry(broker)
    await declare_topology(broker, container.retry_policy)
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
