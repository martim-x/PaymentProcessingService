"""Общее подключение к брокеру с ретраями (общий код relay и consumer)."""

import asyncio
import logging

from faststream.rabbit import RabbitBroker

logger = logging.getLogger(__name__)


async def connect_with_retry(
    broker: RabbitBroker,
    attempts: int = 10,
    delay: float = 2.0,
) -> None:
    """RabbitMQ может отчитаться healthy раньше, чем откроет AMQP-порт."""
    last: Exception | None = None
    for attempt in range(
        1,
        attempts + 1,
    ):
        try:
            await broker.connect()
            return
        except (
            Exception  # noqa: BLE001 — любая ошибка соединения: ждём и пробуем снова
        ) as exc:
            last = exc
            logger.warning(
                "broker connect failed (%d/%d): %s",
                attempt,
                attempts,
                exc,
            )
            await asyncio.sleep(delay)
    if last is None:  # pragma: no cover — цикл всегда завершается исключением
        raise RuntimeError("broker connect failed without an exception")
    raise last
