"""RabbitMQ queue naming, retry policy application, and declarations."""

from faststream.rabbit import RabbitBroker, RabbitQueue

from payment.domain.policies.retry_policy import ExponentialBackoffRetryPolicy


class RabbitTopology:
    """Own RabbitMQ queue names, retry queues, and broker declarations."""

    MAIN_QUEUE = "payments.new"
    DLQ_QUEUE = "payments.dlq"

    @staticmethod
    def retry_queue_prefix(queue_name: str) -> str:
        """Return the prefix shared by regular retry queues."""
        return f"{queue_name}.retry."

    @staticmethod
    def retry_queue_name(queue_name: str, attempt: int) -> str:
        return f"{RabbitTopology.retry_queue_prefix(queue_name)}{attempt}"

    @staticmethod
    def infra_retry_queue_name(queue_name: str, attempt: int) -> str:
        """Return an infrastructure retry queue name."""
        return f"{queue_name}.infra.retry.{attempt}"

    @staticmethod
    def retry_attempt_from_queue(queue_name: str, candidate: str) -> int | None:
        """Parse a regular retry queue name; ignore infra/foreign queues."""
        prefix = RabbitTopology.retry_queue_prefix(queue_name)
        if not candidate.startswith(prefix):
            return None
        suffix = candidate.removeprefix(prefix)
        return int(suffix) if suffix.isdecimal() else None

    @staticmethod
    def retry_delay(
        attempt: int,
        policy: ExponentialBackoffRetryPolicy | None = None,
    ) -> float:
        """Return the policy delay, preserving the default retry policy."""
        return (policy or ExponentialBackoffRetryPolicy()).delay_for_attempt(
            attempt=attempt
        )

    @staticmethod
    def main_queue(
        queue_name: str | None = None,
        dlq_name: str | None = None,
    ) -> RabbitQueue:
        queue_name = RabbitTopology.MAIN_QUEUE if queue_name is None else queue_name
        dlq_name = RabbitTopology.DLQ_QUEUE if dlq_name is None else dlq_name
        return RabbitQueue(
            name=queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": dlq_name,
            },
        )

    @staticmethod
    def retry_queue(
        attempt: int,
        policy: ExponentialBackoffRetryPolicy | None = None,
        queue_name: str | None = None,
    ) -> RabbitQueue:
        """Declare-by-caller queue with TTL then dead-letter to main."""
        queue_name = RabbitTopology.MAIN_QUEUE if queue_name is None else queue_name
        return RabbitQueue(
            name=RabbitTopology.retry_queue_name(queue_name, attempt),
            durable=True,
            arguments={
                "x-message-ttl": int(
                    RabbitTopology.retry_delay(attempt, policy) * 1000
                ),
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )

    @staticmethod
    def infra_retry_queue(
        attempt: int,
        policy: ExponentialBackoffRetryPolicy | None = None,
        queue_name: str | None = None,
    ) -> RabbitQueue:
        """Create the retry queue for transient infrastructure failures."""
        queue_name = RabbitTopology.MAIN_QUEUE if queue_name is None else queue_name
        return RabbitQueue(
            name=RabbitTopology.infra_retry_queue_name(queue_name, attempt),
            durable=True,
            arguments={
                "x-message-ttl": int(
                    RabbitTopology.retry_delay(attempt, policy) * 1000
                ),
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )

    @staticmethod
    def dlq_queue(dlq_name: str | None = None) -> RabbitQueue:
        dlq_name = RabbitTopology.DLQ_QUEUE if dlq_name is None else dlq_name
        return RabbitQueue(name=dlq_name, durable=True)

    @staticmethod
    async def declare_topology(
        broker: RabbitBroker,
        policy: ExponentialBackoffRetryPolicy | None = None,
        queue_name: str | None = None,
        dlq_name: str | None = None,
        *,
        infra_policy: ExponentialBackoffRetryPolicy | None = None,
    ) -> None:
        """Declare main, DLQ and retry ladders for consumer and relay."""
        queue_name = RabbitTopology.MAIN_QUEUE if queue_name is None else queue_name
        dlq_name = RabbitTopology.DLQ_QUEUE if dlq_name is None else dlq_name
        policy = policy or ExponentialBackoffRetryPolicy()
        queues = [
            RabbitTopology.main_queue(queue_name, dlq_name),
            RabbitTopology.dlq_queue(dlq_name),
            *(
                RabbitTopology.retry_queue(attempt, policy, queue_name)
                for attempt in range(1, policy.max_attempts)
            ),
        ]
        for queue in queues:
            await broker.declare_queue(queue)
        if infra_policy is not None:
            for attempt in range(1, policy.max_attempts):
                await broker.declare_queue(
                    RabbitTopology.infra_retry_queue(
                        attempt,
                        infra_policy,
                        queue_name,
                    )
                )
