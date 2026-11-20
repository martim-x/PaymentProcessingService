"""Инфраструктурные сбои consumer'а: отдельный счётчик, свой backoff, DLQ+лог.

Регрессия из code review: любое исключение (включая OperationalError БД)
считалось попыткой обработки, TTL 1/2 секунды, после трёх попыток сообщение
уходило в DLQ, а платёж оставался `pending`. Горячий бесконечный requeue — тоже
не решение: инфраструктурные сбои получают собственную ограниченную цепочку
(`payments.new.infra.retry.N`, заголовок `x-infra-attempt`), а исчерпание
приводит в durable DLQ и лог с id платежа.
"""

import json
import logging
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from payment.adapters.messaging.routing import ATTEMPT_HEADER, INFRA_ATTEMPT_HEADER
from payment.adapters.messaging.topology import declare_topology, infra_retry_queue_name
from payment.application.dto import (
    CreatePaymentCommand,
    ProcessPaymentResult,
    WebhookDeliveryCommand,
)
from payment.application.exceptions import PaymentBusy, PaymentNotFound, WebhookDeliveryExhausted
from payment.config import Settings
from payment.domain.model.enums import Currency
from payment.domain.model.value_objects import IdempotencyKey
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy
from payment.entrypoints.consumer.app import handle_payment_created

PAYMENT_ID = uuid4()
QUEUE = "payments.new"
DLQ = "payments.dlq"
INFRA_QUEUE = "payments.new.infra.retry"
LOGGER = "payment.entrypoints.consumer.app"


def settings(**overrides) -> Settings:
    return Settings(
        _env_file=None,
        api_key="test-api-key",
        webhook_secret="test-webhook-secret",
        **overrides,
    )


class FakeMessage:
    def __init__(self, body: object, headers: dict | None = None) -> None:
        self.body = body
        self.headers = dict(headers or {})
        self.acked = False
        self.requeued = False

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:  # noqa: FBT001, FBT002
        self.requeued = requeue


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[dict] = []

    async def publish(self, payload, *, queue: str, **kwargs) -> bool:
        self.published.append({"payload": payload, "queue": queue, **kwargs})
        return True

    @property
    def last(self) -> dict:
        return self.published[-1]


class StubPaymentService:
    def __init__(self, exc: BaseException | None = None, *, webhook_missing: bool = False) -> None:
        self.exc = exc
        self.webhook_missing = webhook_missing
        self.calls = 0

    async def process_payment(self, payment_id) -> object:
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return ProcessPaymentResult(
            payment_id=payment_id,
            status="succeeded",
            webhook_delivery=None
            if self.webhook_missing
            else WebhookDeliveryCommand(
                event_id="evt-success",
                payment_id=payment_id.value,
                url="https://example.com/hook",
                body={"id": str(payment_id.value), "status": "succeeded"},
            ),
        )


class FakeWebhookService:
    def __init__(self, result=None, exc: BaseException | None = None) -> None:
        self.commands: list[WebhookDeliveryCommand] = []
        self.result = result
        self.exc = exc

    async def deliver(self, command: WebhookDeliveryCommand) -> object:
        self.commands.append(command)
        if self.exc is not None:
            raise self.exc
        return self.result

    async def enqueue(self, command: WebhookDeliveryCommand) -> None:
        self.commands.append(command)


class FakeContainer:
    def __init__(
        self,
        cfg: Settings,
        payment_service: StubPaymentService,
        *,
        stale: bool = False,
        webhook_exc: BaseException | None = None,
    ) -> None:
        self.settings = cfg
        self.retry_policy = ExponentialBackoffRetryPolicy(
            max_attempts=cfg.max_attempts,
            base_delay=cfg.retry_base_delay,
            factor=cfg.retry_factor,
        )
        self._payment_service = payment_service
        self._webhook_service = FakeWebhookService(SimpleNamespace(stale=stale), exc=webhook_exc)

    def payment_service(self) -> StubPaymentService:
        return self._payment_service

    def webhook_service(self) -> FakeWebhookService:
        return self._webhook_service


def body() -> dict:
    return {"event": "payment.created", "payment_id": str(PAYMENT_ID)}


async def consume(
    container: FakeContainer, *, headers: dict | None = None
) -> tuple[FakeMessage, FakeBroker]:
    message = FakeMessage(json.dumps(body()).encode(), headers=headers)
    broker = FakeBroker()
    await handle_payment_created(message, broker=broker, container=container)  # type: ignore[arg-type]
    return message, broker


class TestInfraRouting:
    async def test_stale_webhook_result_routes_busy_without_spending_attempt(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(), stale=True)

        message, broker = await consume(
            container, headers={ATTEMPT_HEADER: 2, INFRA_ATTEMPT_HEADER: 3}
        )

        assert message.acked is True
        assert broker.last["queue"] == f"{QUEUE}.retry.2"
        assert broker.last["headers"] == {
            ATTEMPT_HEADER: 2,
            INFRA_ATTEMPT_HEADER: 3,
        }

    async def test_operational_error_uses_its_own_retry_chain(self) -> None:
        container = FakeContainer(
            settings(), StubPaymentService(OperationalError("q", {}, Exception("x")))
        )

        message, broker = await consume(container)

        assert message.acked is True
        assert broker.last["queue"] == infra_retry_queue_name(QUEUE, 1)
        assert broker.last["headers"] == {INFRA_ATTEMPT_HEADER: 1}
        assert ATTEMPT_HEADER not in broker.last["headers"]  # business-счётчик не тронут

    async def test_infra_delay_step_uses_its_own_counter(self) -> None:
        container = FakeContainer(
            settings(max_attempts=5),
            StubPaymentService(OperationalError("q", {}, Exception("x"))),
        )

        _, broker = await consume(container, headers={ATTEMPT_HEADER: 2, INFRA_ATTEMPT_HEADER: 1})

        assert broker.last["queue"] == infra_retry_queue_name(QUEUE, 2)
        assert broker.last["headers"] == {
            ATTEMPT_HEADER: 2,
            INFRA_ATTEMPT_HEADER: 2,
        }

    async def test_business_failure_shares_the_budget_with_infra_failures(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(RuntimeError("gateway failure")))

        _, broker = await consume(container, headers={ATTEMPT_HEADER: 1, INFRA_ATTEMPT_HEADER: 1})

        # 1 business + 1 infra отказ = 2 израсходованные попытки; третий отказ — предел
        assert broker.last["queue"] == DLQ
        assert broker.last["headers"] is None

    async def test_business_retry_preserves_infra_counter(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(RuntimeError("gateway failure")))

        _, broker = await consume(container, headers={ATTEMPT_HEADER: 1, INFRA_ATTEMPT_HEADER: 0})

        assert broker.last["queue"] == f"{QUEUE}.retry.2"
        assert broker.last["headers"] == {
            ATTEMPT_HEADER: 2,
            INFRA_ATTEMPT_HEADER: 0,
        }

    async def test_infra_exhaustion_sends_message_to_the_durable_dlq(self, caplog) -> None:
        cfg = settings()
        container = FakeContainer(
            cfg, StubPaymentService(OperationalError("q", {}, Exception("x")))
        )

        with caplog.at_level(logging.ERROR, logger=LOGGER):
            message, broker = await consume(
                container, headers={INFRA_ATTEMPT_HEADER: cfg.max_attempts - 1}
            )

        assert message.acked is True
        assert broker.last["queue"] == DLQ
        assert broker.last["headers"] is None
        assert broker.last["persist"] is True
        assert str(PAYMENT_ID) in caplog.text
        assert "infra" in caplog.text.lower()

    async def test_payment_busy_retries_without_spending_an_attempt(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(PaymentBusy(str(PAYMENT_ID))))

        message, broker = await consume(container)

        assert message.acked is True
        assert broker.last["queue"] == f"{QUEUE}.retry.1"
        assert broker.last["headers"] == {ATTEMPT_HEADER: 0}

    async def test_payment_busy_does_not_spend_business_attempt(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(PaymentBusy(str(PAYMENT_ID))))

        message, broker = await consume(container, headers={ATTEMPT_HEADER: 2})

        assert message.acked is True
        assert broker.last["queue"] == f"{QUEUE}.retry.2"
        assert broker.last["headers"] == {ATTEMPT_HEADER: 2}

    async def test_unknown_error_is_bounded_by_the_payment_chain(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(RuntimeError("boom")))

        _, broker = await consume(container)

        assert broker.last["queue"] == f"{QUEUE}.retry.1"
        assert broker.last["headers"] == {ATTEMPT_HEADER: 1}
        assert INFRA_ATTEMPT_HEADER not in broker.last["headers"]

    async def test_not_found_is_not_retried_as_infrastructure(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(PaymentNotFound(str(PAYMENT_ID))))

        _, broker = await consume(container)

        assert broker.last["queue"] == f"{QUEUE}.retry.1"
        assert broker.last["headers"] == {ATTEMPT_HEADER: 1}

    async def test_missing_webhook_payload_is_retried_not_acked(self) -> None:
        container = FakeContainer(settings(), StubPaymentService(webhook_missing=True))

        message, broker = await consume(container)

        assert message.acked is True  # original transferred only after retry publish
        assert broker.last["queue"] == f"{QUEUE}.retry.1"
        assert broker.last["headers"] == {ATTEMPT_HEADER: 1}
        assert container.webhook_service().commands == []

    async def test_success_acks_without_redelivery(self) -> None:
        container = FakeContainer(settings(), StubPaymentService())

        message, broker = await consume(container)

        assert message.acked is True
        assert broker.published == []
        assert len(container.webhook_service().commands) == 1
        assert container.webhook_service().commands[0].payment_id == PAYMENT_ID

    async def test_exhausted_webhook_goes_straight_to_dlq(self) -> None:
        """Исчерпанный бюджет доставки не тратит попытки брокера."""
        container = FakeContainer(
            settings(),
            StubPaymentService(),
            webhook_exc=WebhookDeliveryExhausted("evt-success", 3, "endpoint is down"),
        )

        message, broker = await consume(container)

        assert message.acked is True
        assert broker.last["queue"] == DLQ
        assert broker.last["headers"] is None


class TestFailureChain:
    async def test_alternating_failures_share_one_budget(self) -> None:
        """business → infra → business: счётчики переносятся, третий отказ — DLQ."""
        from payment.adapters.messaging.redelivery import publish_failure
        from payment.adapters.messaging.routing import route_failure, route_infra_failure

        policy = ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=1.0)
        headers: dict = {}
        actions: list[str] = []

        for route in (route_failure, route_infra_failure, route_failure):
            message = FakeMessage(json.dumps(body()).encode(), headers=headers)
            broker = FakeBroker()
            decision = route(headers, policy, queue_name=QUEUE, dlq_name=DLQ)
            await publish_failure(broker, message, body(), decision)  # type: ignore[arg-type]
            actions.append(decision.action)
            if decision.action == "dlq":
                assert broker.last["queue"] == DLQ
                assert broker.last["headers"] is None
                break
            headers = broker.last["headers"]

        assert actions == ["retry", "retry", "dlq"]


class TestConsumerRedelivery:
    """Consumer-уровень: повтор сообщения после финализации не ходит в шлюз."""

    async def test_redelivery_after_webhook_failure_skips_the_gateway(self) -> None:
        from payment.application.services.payment_service import PaymentService
        from payment.application.services.webhook_service import WebhookService
        from tests.fakes import FakeGateway, FakeUnitOfWork, FakeWebhookSender

        uow = FakeUnitOfWork()
        gateway = FakeGateway(succeed=True)
        sender = FakeWebhookSender(fail_first=1)

        class RealContainer:
            def __init__(self) -> None:
                self.settings = settings()
                self.retry_policy = ExponentialBackoffRetryPolicy(
                    max_attempts=self.settings.max_attempts,
                    base_delay=self.settings.retry_base_delay,
                    factor=self.settings.retry_factor,
                )

            def payment_service(self) -> PaymentService:
                return PaymentService(uow=uow, gateway=gateway)

            def webhook_service(self) -> WebhookService:
                return WebhookService(
                    uow=uow, sender=sender, max_attempts=self.settings.max_attempts
                )

        container = RealContainer()
        created = await container.payment_service().create_payment(
            CreatePaymentCommand(
                amount=Decimal("10.00"),
                currency=Currency.RUB,
                description="redelivery",
                metadata={},
                idempotency_key=IdempotencyKey("redelivery-key"),
                webhook_url="https://example.com/hook",
            )
        )
        payment_id = str(created.payment.id)
        first = FakeMessage(
            json.dumps({"event": "payment.created", "payment_id": payment_id}).encode()
        )
        broker = FakeBroker()
        await handle_payment_created(first, broker=broker, container=container)  # type: ignore[arg-type]

        assert first.acked is True
        assert broker.last["queue"] == f"{QUEUE}.retry.1"
        assert gateway.calls == 1
        assert sender.calls == 1  # первая HTTP-попытка упала

        second = FakeMessage(
            json.dumps({"event": "payment.created", "payment_id": payment_id}).encode(),
            headers=broker.last["headers"],
        )
        retry_broker = FakeBroker()
        await handle_payment_created(second, broker=retry_broker, container=container)  # type: ignore[arg-type]

        assert second.acked is True
        assert retry_broker.published == []  # повтор не породил новое сообщение
        assert gateway.calls == 1  # шлюз не вызывался второй раз
        assert sender.calls == 2
        assert len(sender.sent) == 1
        assert sender.sent[0]["payload"]["id"] == payment_id


class TestTopologyDeclaration:
    class RecordingBroker:
        def __init__(self) -> None:
            self.declared: dict[str, object] = {}

        async def declare_queue(self, queue) -> None:
            self.declared[queue.name] = queue

    async def test_infra_retry_chain_is_declared_with_its_own_backoff(self) -> None:
        cfg = settings()
        broker = self.RecordingBroker()
        fast = ExponentialBackoffRetryPolicy(
            max_attempts=cfg.max_attempts,
            base_delay=cfg.retry_base_delay,
            factor=cfg.retry_factor,
        )

        await declare_topology(
            broker,  # type: ignore[arg-type]
            fast,
            QUEUE,
            DLQ,
            infra_policy=cfg.infra_retry_policy,
        )

        infra_names = sorted(name for name in broker.declared if name.startswith(f"{INFRA_QUEUE}."))
        assert infra_names == [
            infra_retry_queue_name(QUEUE, attempt) for attempt in range(1, cfg.max_attempts)
        ]
        first = broker.declared[infra_retry_queue_name(QUEUE, 1)]
        second = broker.declared[infra_retry_queue_name(QUEUE, 2)]
        assert first.arguments["x-message-ttl"] == int(cfg.infra_retry_base_delay * 1000)
        assert second.arguments["x-message-ttl"] == int(
            cfg.infra_retry_base_delay * cfg.retry_factor * 1000
        )
        assert second.arguments["x-dead-letter-routing-key"] == QUEUE
        assert second.durable is True

    async def test_no_infra_chain_without_a_policy(self) -> None:
        cfg = settings()
        broker = self.RecordingBroker()

        await declare_topology(
            broker,  # type: ignore[arg-type]
            cfg.infra_retry_policy,
            QUEUE,
            DLQ,
            infra_policy=None,
        )

        assert not [name for name in broker.declared if ".infra.retry." in name]


class TestRoutingPolicyUnit:
    @pytest.mark.parametrize(
        ("business", "infra", "failure", "expected_action", "expected_queue"),
        [
            (1, 1, "business", "dlq", DLQ),
            (1, 1, "infra", "dlq", DLQ),
            (0, 0, "business", "retry", f"{QUEUE}.retry.1"),
            (0, 0, "infra", "retry", infra_retry_queue_name(QUEUE, 1)),
            (0, 2, "business", "dlq", DLQ),
            (2, 0, "infra", "dlq", DLQ),
        ],
    )
    def test_failure_types_share_one_total_budget(
        self, business, infra, failure, expected_action, expected_queue
    ) -> None:
        from payment.adapters.messaging.routing import route_failure, route_infra_failure

        policy = ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=2.0)
        headers = {ATTEMPT_HEADER: business, INFRA_ATTEMPT_HEADER: infra}
        route = route_failure if failure == "business" else route_infra_failure

        decision = route(headers, policy, queue_name=QUEUE, dlq_name=DLQ)

        assert decision.action == expected_action
        assert decision.queue == expected_queue
        assert decision.attempt == business + infra + 1
        own = business if failure == "business" else infra
        assert decision.header_value == own + 1

    def test_infra_attempt_header_is_separate(self) -> None:
        from payment.adapters.messaging.routing import route_infra_failure

        decision = route_infra_failure(
            {ATTEMPT_HEADER: 2},
            ExponentialBackoffRetryPolicy(max_attempts=5, base_delay=2.0),
            queue_name=QUEUE,
            dlq_name=DLQ,
        )

        assert decision.action == "retry"
        # бюджет считает оба класса отказов, а ступень задержки — только инфраструктурные
        assert decision.attempt == 3
        assert decision.header_value == 1
        assert decision.queue == infra_retry_queue_name(QUEUE, 1)
        assert decision.header == INFRA_ATTEMPT_HEADER

    @pytest.mark.parametrize("header_value", [1, 2])
    def test_infra_chain_step_follows_the_infra_counter(self, header_value: int) -> None:
        from payment.adapters.messaging.routing import route_infra_failure

        policy = ExponentialBackoffRetryPolicy(max_attempts=4, base_delay=2.0)
        decision = route_infra_failure(
            {INFRA_ATTEMPT_HEADER: header_value}, policy, queue_name=QUEUE, dlq_name=DLQ
        )

        assert decision.queue == infra_retry_queue_name(QUEUE, header_value + 1)
        assert decision.action == "retry"
        assert decision.header == INFRA_ATTEMPT_HEADER
        assert decision.header_value == header_value + 1

    def test_busy_clamps_to_the_last_retry_queue(self) -> None:
        """Занятое сообщение откладывается по последней ступени и не тратит бюджет."""
        from payment.adapters.messaging.routing import route_busy

        decision = route_busy(
            {ATTEMPT_HEADER: 9},
            ExponentialBackoffRetryPolicy(max_attempts=3, base_delay=1.0),
            queue_name=QUEUE,
        )

        assert decision.action == "retry"
        assert decision.queue == f"{QUEUE}.retry.2"
        assert decision.header_value == 9

    def test_total_failures_reach_dlq_at_the_shared_limit(self) -> None:
        from payment.adapters.messaging.routing import route_infra_failure

        policy = ExponentialBackoffRetryPolicy(max_attempts=4, base_delay=2.0)
        decision = route_infra_failure(
            {INFRA_ATTEMPT_HEADER: 3}, policy, queue_name=QUEUE, dlq_name=DLQ
        )

        assert decision.action == "dlq"
        assert decision.queue == DLQ
        assert decision.attempt == 4
