"""FastStream-приложение consumer'а: очередь payments.new.

Один обработчик делает всё: получает сообщение, обрабатывает платёж,
обновляет статус и отправляет webhook. Инфраструктурные сбои (БД и т.п.)
уходят в retry-очереди с экспоненциальным TTL, после 3 попыток — в DLQ.
"""

import asyncio
import json
import logging
import signal

from faststream.rabbit import RabbitBroker, RabbitMessage

from payment.adapters.messaging.schemas import PaymentCreatedMessage
from payment.adapters.messaging.topology import (
    DLQ_QUEUE,
    MAX_ATTEMPTS,
    RETRY_QUEUE_FMT,
    dlq_queue,
    main_queue,
    retry_queue,
)
from payment.application.use_cases.process_payment import ProcessPayment
from payment.bootstrap import Container
from payment.entrypoints.consumer.handlers import is_retry_exhausted, parse_payment_id

logger = logging.getLogger(__name__)


def attempt_from_headers(headers: dict) -> int:
    """Сколько раз сообщение уже побывало в retry-очередях (по x-death)."""
    attempts = 0
    for death in headers.get("x-death") or []:
        if str(death.get("queue", "")).startswith("payments.new.retry."):
            attempts += int(death.get("count", 1))
    return attempts


async def _ack(message: RabbitMessage) -> None:
    try:
        await message.ack()
    except Exception:  # noqa: S110 — ack может упасть при разрыве соединения
        logger.debug("ack failed", exc_info=True)


def build_consumer_app(container: Container | None = None) -> RabbitBroker:
    container = container or Container()
    broker = container.get_broker()

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
            attempt = attempt_from_headers(message.headers) + 1
            if not is_retry_exhausted(attempt, MAX_ATTEMPTS):
                target = RETRY_QUEUE_FMT.format(attempt=attempt)
                logger.warning(
                    "payment %s failed (%s); retry %d via %s",
                    event.payment_id,
                    exc,
                    attempt,
                    target,
                )
                await broker.publish(payload, queue=target)
            else:
                logger.error(
                    "payment %s failed %d times (%s); -> DLQ", event.payment_id, attempt, exc
                )
                await broker.publish(payload, queue=DLQ_QUEUE)
            await _ack(message)

    return broker


async def declare_topology(broker: RabbitBroker) -> None:
    """Объявляет retry-очереди и DLQ (main объявит сам subscriber)."""
    for q in (*[retry_queue(i) for i in range(1, MAX_ATTEMPTS)], dlq_queue()):
        await broker.declare_queue(q)


async def run_consumer(container: Container | None = None) -> None:
    """Точка входа consumer-процесса (retry подключения к брокеру при старте)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    container = container or Container()
    broker = build_consumer_app(container)
    await _connect_with_retry(broker)
    await declare_topology(broker)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await broker.start()  # объявляет подписки; start() не блокирует
        await stop.wait()  # живём, пока не придёт сигнал
    finally:
        await broker.stop()


async def _connect_with_retry(broker: RabbitBroker, attempts: int = 10, delay: float = 2.0) -> None:
    """RabbitMQ может отчитаться healthy раньше, чем откроет AMQP-порт."""
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            await broker.connect()
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            logger.warning("broker connect failed (%d/%d): %s", attempt, attempts, exc)
            await asyncio.sleep(delay)
    assert last is not None
    raise last


if __name__ == "__main__":
    asyncio.run(run_consumer())
