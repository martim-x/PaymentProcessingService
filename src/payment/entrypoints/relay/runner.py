"""Relay: поллинг outbox и публикация в payments.new."""

import asyncio
import logging
import signal
from types import FrameType

from payment.adapters.messaging.connection import connect_with_retry
from payment.adapters.messaging.topology import declare_topology
from payment.bootstrap import Container

logger = logging.getLogger(__name__)


async def run_relay(
    container: Container,
    *,
    interval: float = 1.0,
    batch: int = 100,
) -> None:
    """Каждые `interval` секунд берёт пакет неотправленных записей outbox и публикует их."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal(
        sig: signal.Signals,
        frame: FrameType | None,
    ) -> None:
        logger.info(
            "relay stopping (signal %s)",
            sig,
        )
        stop.set()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        loop.add_signal_handler(
            sig,
            _on_signal,
            sig,
            None,
        )

    broker = container.get_broker()
    await connect_with_retry(broker)
    # топологию объявляет и relay: публикация в необъявленную очередь теряется
    await declare_topology(
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
                    logger.info(
                        "relay published %d events",
                        published,
                    )
            except Exception:  # noqa: BLE001 — одна итерация не должна убивать relay
                logger.exception("relay iteration failed; will retry after interval")
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=interval,
                )
            except TimeoutError:
                pass
    finally:
        await broker.stop()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    container = Container()
    asyncio.run(
        run_relay(
            container,
            interval=container.settings.relay_interval,
            batch=container.settings.relay_batch,
        )
    )


if __name__ == "__main__":
    main()
