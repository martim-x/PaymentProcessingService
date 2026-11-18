"""Relay: поллинг outbox и публикация в payments.new."""

import asyncio
import logging
import signal
from types import FrameType

from payment.bootstrap import Container

logger = logging.getLogger(__name__)


async def run_relay(container: Container, *, interval: float = 1.0, batch: int = 100) -> None:
    """Каждые `interval` сек берёт пакет несент записей outbox и публикует их."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal(sig: signal.Signals, frame: FrameType | None) -> None:
        logger.info("relay stopping (signal %s)", sig)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _on_signal, sig, None)

    broker = container.get_broker()
    await _connect_with_retry(broker)
    try:
        while not stop.is_set():
            try:
                use_case = container.publish_outbox()
                published = await use_case.execute(batch=batch)
                if published:
                    logger.info("relay published %d events", published)
            except Exception:
                logger.exception("relay iteration failed; will retry after interval")
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                pass
    finally:
        await broker.stop()


async def _connect_with_retry(broker, attempts: int = 10, delay: float = 2.0) -> None:
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


def main() -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
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
