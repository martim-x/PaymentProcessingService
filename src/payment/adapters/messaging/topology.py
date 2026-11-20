"""Топология RabbitMQ: один конвейер — платежи (обработка + webhook).

Схема (direct-маршрутизация через default exchange, по имени очереди):

    <queue>                 --consumer-->  обработка платежа и webhook
       ^                                        | ошибка
       | dead-letter (TTL истёк)                v
    <queue>.retry.<n>       <-- publish -- retry-очередь
       (TTL = base * factor ** (n-1))           | после max_attempts отказов
                                                v
                                             <dlq>

Две лестницы задержек: обычная `<queue>.retry.N` и более медленная
`<queue>.infra.retry.N` для транзиентных сбоев БД/транспорта. Бюджет отказов у
них общий (`max_attempts`), поэтому любая лестница объявляется ровно на
max_attempts - 1 ступеней: сообщение не может уйти в несуществующую очередь.

Имена очередей приходят из настроек (PAYMENT_RABBITMQ_QUEUE / PAYMENT_RABBITMQ_DLQ):
объявление и публикация обязаны использовать одни и те же имена. Топологию
объявляют И consumer, И relay — иначе публикация в необъявленную очередь через
default exchange теряется, а outbox-запись помечается sent.
"""

from faststream.rabbit import RabbitBroker, RabbitQueue

from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

MAIN_QUEUE = "payments.new"
DLQ_QUEUE = "payments.dlq"

MAX_ATTEMPTS = ExponentialBackoffRetryPolicy().max_attempts


def retry_queue_prefix(queue_name: str) -> str:
    """Префикс обычных retry-очередей, общий для создания и разбора имён."""
    return f"{queue_name}.retry."


def retry_queue_name(queue_name: str, attempt: int) -> str:
    return f"{retry_queue_prefix(queue_name)}{attempt}"


def infra_retry_queue_name(queue_name: str, attempt: int) -> str:
    """Retry-очередь транзиентных инфраструктурных сбоев (своя лестница TTL)."""
    return f"{queue_name}.infra.retry.{attempt}"


def retry_attempt_from_queue(queue_name: str, candidate: str) -> int | None:
    """Номер обычной retry-очереди; infra-очереди и посторонние имена дают None."""
    prefix = retry_queue_prefix(queue_name)
    if not candidate.startswith(prefix):
        return None
    suffix = candidate.removeprefix(prefix)
    return int(suffix) if suffix.isdecimal() else None


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


def infra_retry_queue(
    attempt: int,
    policy: ExponentialBackoffRetryPolicy | None = None,
    queue_name: str = MAIN_QUEUE,
) -> RabbitQueue:
    """Очередь задержки для инфраструктурного сбоя: свой TTL из своей политики."""
    return RabbitQueue(
        name=infra_retry_queue_name(queue_name, attempt),
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay(attempt, policy) * 1000),
            "x-dead-letter-exchange": "",
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
    *,
    infra_policy: ExponentialBackoffRetryPolicy | None = None,
) -> None:
    """Объявляет main-очередь, её retry-лестницу, DLQ и (опционально) infra-лестницу.

    Обе лестницы объявляются на `policy.max_attempts - 1` ступеней: общий бюджет
    отказов не позволяет сообщению проскочить в несуществующую очередь. Без
    `infra_policy` публикация инфраструктурного retry попала бы в
    необъявленную очередь и потерялась (mandatory + `on_return_raises`).
    """
    policy = policy or ExponentialBackoffRetryPolicy()
    queues = [main_queue(queue_name, dlq_name), dlq_queue(dlq_name)]
    queues += [
        retry_queue(attempt, policy, queue_name) for attempt in range(1, policy.max_attempts)
    ]
    for queue in queues:
        await broker.declare_queue(queue)
    if infra_policy is not None:
        for attempt in range(1, policy.max_attempts):
            await broker.declare_queue(infra_retry_queue(attempt, infra_policy, queue_name))
