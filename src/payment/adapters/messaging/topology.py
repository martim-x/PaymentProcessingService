"""Топология RabbitMQ: основная очередь, retry-очереди с TTL, DLQ.

Схема (direct-маршрутизация через default exchange, по имени очереди):

    payments.new            --consumer-->  обработка
       ^                                        | ошибка
       | dead-letter (TTL истёк)                v
    payments.new.retry.<n>  <-- publish -- payments.new.retry.<n>
       (TTL = base * factor ** (n-1))           | после N попыток
                                                v
                                          payments.dlq

Задержки берутся из ExponentialBackoffRetryPolicy (PAYMENT_RETRY_*).
Топологию объявляют И consumer, И relay: публикация в необъявленную очередь
через default exchange молча теряется, а outbox-запись помечается sent.
"""

from faststream.rabbit import RabbitBroker, RabbitQueue

from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

MAIN_QUEUE = "payments.new"
RETRY_QUEUE_FMT = "payments.new.retry.{attempt}"
DLQ_QUEUE = "payments.dlq"

MAX_ATTEMPTS = ExponentialBackoffRetryPolicy().max_attempts


def retry_delay(attempt: int, policy: ExponentialBackoffRetryPolicy | None = None) -> float:
    """Экспоненциальная задержка (сек): 1 -> 1с, 2 -> 2с, 3 -> 4с (по умолчанию)."""
    return (policy or ExponentialBackoffRetryPolicy()).delay_for_attempt(attempt)


def main_queue() -> RabbitQueue:
    return RabbitQueue(
        name=MAIN_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",  # default exchange
            "x-dead-letter-routing-key": DLQ_QUEUE,
        },
    )


def retry_queue(attempt: int, policy: ExponentialBackoffRetryPolicy | None = None) -> RabbitQueue:
    """Очередь задержки для попытки `attempt`: TTL, затем dead-letter в main."""
    name = RETRY_QUEUE_FMT.format(attempt=attempt)
    return RabbitQueue(
        name=name,
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay(attempt, policy) * 1000),
            "x-dead-letter-exchange": "",  # default exchange
            "x-dead-letter-routing-key": MAIN_QUEUE,
        },
    )


def dlq_queue() -> RabbitQueue:
    return RabbitQueue(name=DLQ_QUEUE, durable=True)


async def declare_topology(
    broker: RabbitBroker, policy: ExponentialBackoffRetryPolicy | None = None
) -> None:
    """Объявляет main, retry-очереди и DLQ (идемпотентно для RabbitMQ)."""
    policy = policy or ExponentialBackoffRetryPolicy()
    queues = [main_queue(), dlq_queue()]
    queues += [retry_queue(i, policy) for i in range(1, policy.max_attempts)]
    for queue in queues:
        await broker.declare_queue(queue)
