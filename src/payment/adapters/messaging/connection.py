"""Общее подключение к брокеру с ретраями (общий код relay и consumer)."""

import asyncio
import logging

from faststream.rabbit import RabbitBroker


class BrokerConnector:
    """Владелец политики подключения к RabbitMQ с повторными попытками."""

    _logger = logging.getLogger(__name__)

    @staticmethod
    async def connect_with_retry(
        broker: RabbitBroker,
        attempts: int = 10,
        delay: float = 2.0,
    ) -> None:
        """RabbitMQ может отчитаться healthy раньше, чем откроет AMQP-порт."""
        last: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                await broker.connect()
                return
            except Exception as exc:  # noqa: BLE001 — любая ошибка соединения
                last = exc
                BrokerConnector._logger.warning(
                    "broker connect failed (%d/%d): %s",
                    attempt,
                    attempts,
                    exc,
                )
                await asyncio.sleep(delay)
        if last is None:  # pragma: no cover — цикл всегда завершается исключением
            raise RuntimeError("broker connect failed without an exception")
        raise last
