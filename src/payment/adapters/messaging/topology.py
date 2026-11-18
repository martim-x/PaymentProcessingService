"""Топология RabbitMQ: основная очередь, retry-очереди с TTL, DLQ.

Схема (direct-маршрутизация через default exchange, по имени очереди):

    <queue>                 --consumer-->  обработка
       ^                                        | ошибка
       | dead-letter (TTL истёк)                v
    <queue>.retry.<n>       <-- publish -- <queue>.retry.<n>
       (TTL = base * factor ** (n-1))           | после N попыток
                                                v
                                             <dlq>

Имена очередей приходят из настроек (PAYMENT_RABBITMQ_QUEUE / PAYMENT_RABBITMQ_DLQ):
объявление и публикация обязаны использовать одни и те же имена. Топологию
объявляют И consumer, И relay — иначе публикация в необъявленную очередь
через default exchange теряется, а outbox-запись помечается sent.
"""

from faststream.rabbit import RabbitBroker, RabbitQueue

from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

MAIN_QUEUE = "payments.new"
DLQ_QUEUE = "payments.dlq"
MAX_ATTEMPTS = ExponentialBackoffRetryPolicy().max_attempts


def retry_queue_name(queue_name: str, attempt: int) -> str:
    return f"{queue_name}.retry.{attempt}"


def retry_delay(attempt: int, policy: ExponentialBackoffRetryPolicy | None = None) -> float:
    """Экспоненциальная задержка (сек): 1 -> 1с, 2 -> 2с, 3 -> 4с (по умолчанию)."""
    return (policy or ExponentialBackoffRetryPolicy()).delay_for_attempt(attempt)


def main_queue(queue_name: str = MAIN_QUEUE, dlq_name: str = DLQ_QUEUE) -> RabbitQueue:
    return RabbitQueue(
        name=queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",  # default exchange
            "x-dead-letter-routing-key": dlq_name,
        },
    )


def retry_queue(
    attempt: int,
    policy: ExponentialBackoffRetryPolicy | None = None,
    queue_name: str = MAIN_QUEUE,
) -> RabbitQueue:
    """Очередь задержки для попытки `attempt`: TTL, затем dead-letter в main."""
    return RabbitQueue(
        name=retry_queue_name(queue_name, attempt),
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay(attempt, policy) * 1000),
            "x-dead-letter-exchange": "",  # default exchange
            "x-dead-letter-routing-key": queue_name,
        },
    )


def dlq_queue(dlq_name: str = DLQ_QUEUE) -> RabbitQueue:
    return RabbitQueue(name=dlq_name, durable=True)


async def declare_topology(
    broker: RabbitBroker,
    policy: ExponentialBackoffRetryPolicy | None = None,
    queue_name: str = MAIN_QUEUE,
    dlq_name: str = DLQ_QUEUE,
) -> None:
    """Объявляет main, retry-очереди и DLQ (идемпотентно для RabbitMQ)."""
    policy = policy or ExponentialBackoffRetryPolicy()
    queues = [main_queue(queue_name, dlq_name), dlq_queue(dlq_name)]
    queues += [retry_queue(i, policy, queue_name) for i in range(1, policy.max_attempts)]
    for queue in queues:
        await broker.declare_queue(queue)
