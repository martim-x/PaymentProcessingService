"""Relay: поллинг outbox и публикация в payments.new."""

import asyncio
import logging
import signal
from functools import partial

from payment.adapters.messaging.connection import BrokerConnector
from payment.adapters.messaging.topology import RabbitTopology
from payment.bootstrap import Container

logger = logging.getLogger(__name__)


class RelayRunner:
    """Цикл публикации outbox и остановка процесса по сигналу."""

    @staticmethod
    def _on_signal(
        *,
        sig: signal.Signals,
        stop: asyncio.Event,
    ) -> None:
        logger.info(
            "relay stopping (signal %s)",
            sig,
        )
        stop.set()

    @staticmethod
    async def run(
        *,
        container: Container,
        interval: float = 1.0,
        batch: int = 100,
    ) -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, partial(RelayRunner._on_signal, sig=sig, stop=stop)
            )

        broker = container.get_broker()
        await BrokerConnector.connect_with_retry(broker)
        # Топологию объявляет и relay: очередь должна существовать до публикации.
        await RabbitTopology.declare_topology(
            broker,
            container.retry_policy,
            container.settings.rabbitmq_queue,
            container.settings.rabbitmq_dlq,
            infra_policy=container.settings.infra_retry_policy,
        )
        try:
            while not stop.is_set():
                try:
                    outbox_service = container.outbox_service()
                    published = await outbox_service.publish_pending(batch=batch)
                    if published:
                        logger.info("relay published %d events", published)
                except Exception:  # noqa: BLE001 — продолжить после отказа итерации
                    logger.exception(
                        "relay iteration failed; will retry after interval"
                    )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                except TimeoutError:
                    pass
        finally:
            await broker.stop()

    @staticmethod
    def main() -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        container = Container()
        asyncio.run(
            RelayRunner.run(
                container=container,
                interval=container.settings.relay_interval,
                batch=container.settings.relay_batch,
            )
        )


if __name__ == "__main__":
    RelayRunner.main()
