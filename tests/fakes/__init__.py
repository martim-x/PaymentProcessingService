"""Fake-реализации портов для unit-тестов сервисов приложения.

Всё в одном модуле, чтобы тесты могли делать `from tests.fakes import ...`.
"""

import copy
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from payment.application.ports.payment_gateway import GatewayResult
from payment.application.ports.repositories import (
    OutboxDispatch,
    OutboxRecord,
    WebhookDelivery,
    WebhookDeliveryStatus,
)
from payment.domain.model.enums import PaymentStatus
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId


class FakePaymentRepository:
    def __init__(self) -> None:
        self._by_id: dict[str, Payment] = {}
        self._by_key: dict[str, Payment] = {}
        # имитация гонки на unique index: add() падает IntegrityError
        self.fail_next_add_with_integrity = False
        self.add_calls = 0
        self.idempotency_reads = 0
        self.race_reads: list[Payment | None] = []
        # имитация платежа, уже обработанного другим consumer'ом
        self.finalized_elsewhere: set[str] = set()
        # имитация активного клейма (другой consumer прямо сейчас обрабатывает)
        self.claimed: set[str] = set()

    async def add(
        self,
        *,
        payment: Payment,
    ) -> None:
        self.add_calls += 1
        if self.fail_next_add_with_integrity:
            from payment.application.exceptions import IdempotencyViolation

            self.fail_next_add_with_integrity = False
            raise IdempotencyViolation(idempotency_key=payment.idempotency_key.value)
        self._by_id[str(payment.id)] = payment
        self._by_key[payment.idempotency_key.value] = payment

    async def try_claim(
        self,
        *,
        payment_id: PaymentId,
        ttl_seconds: float,
    ) -> datetime | None:
        key = str(payment_id)
        payment = self._by_id.get(key)
        if payment is None:
            return None
        if payment.status is not PaymentStatus.PENDING:
            return None
        if key in self.claimed:
            return None
        self.claimed.add(key)
        return datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

    async def release_claim(
        self,
        *,
        payment_id,
        claim_token,
    ) -> None:
        self.claimed.discard(str(payment_id))

    async def try_finalize(
        self,
        *,
        payment: Payment,
        claim_token: datetime,
    ) -> bool:
        key = str(payment.id)
        if key in self.finalized_elsewhere:
            return False
        self._by_id[key] = payment
        self.finalized_elsewhere.add(key)
        self.claimed.discard(key)
        return True

    async def get(
        self,
        *,
        payment_id: PaymentId,
    ) -> Payment | None:
        # копия, как строка БД -> новый агрегат: мутация домена не видна до UPDATE
        stored = self._by_id.get(str(payment_id))
        return copy.deepcopy(stored) if stored is not None else None

    async def get_by_idempotency_key(
        self,
        *,
        key: str,
    ) -> Payment | None:
        self.idempotency_reads += 1
        if self.race_reads:
            payment = self.race_reads.pop(0)
            if payment is None:
                return None
            return copy.deepcopy(payment)
        stored = self._by_key.get(key)
        return copy.deepcopy(stored) if stored is not None else None

    def put(
        self,
        *,
        payment: Payment,
    ) -> None:
        """Тестовый хелпер: положить платёж в «БД», не вызывая add()."""
        self._by_id[str(payment.id)] = payment
        self._by_key[payment.idempotency_key.value] = payment


class FakeOutboxRepository:
    def __init__(self) -> None:
        self._records: list[OutboxRecord] = []
        # имитация сбоя INSERT в outbox: транзакция обязана откатиться целиком
        self.fail_next_add = False

    async def find_event(
        self,
        *,
        aggregate_id: str,
        event_type: str,
    ) -> OutboxRecord | None:
        return next(
            (
                r
                for r in self._records
                if r.aggregate_id == aggregate_id and r.event_type == event_type
            ),
            None,
        )

    async def add(
        self,
        *,
        payload: dict[str, object],
        aggregate_id: str,
        dispatch: OutboxDispatch = OutboxDispatch.RELAY,
    ) -> OutboxRecord:
        if self.fail_next_add:
            self.fail_next_add = False
            raise RuntimeError("outbox insert failed")
        record = OutboxRecord(
            id=uuid.uuid4(),
            payload=payload,
            aggregate_id=aggregate_id,
            event_type=str(
                payload.get(
                    "event",
                    "",
                )
            ),
            dispatch=dispatch,
        )
        self._records.append(record)
        return record

    async def fetch_unsent(
        self,
        *,
        batch: int = 100,
    ) -> list[OutboxRecord]:
        """Только публикуемые записи: `dispatch='local'` relay не видит."""
        return [
            r
            for r in self._records
            if not r.sent and not r.failed and r.dispatch == OutboxDispatch.RELAY
        ][:batch]

    async def mark_attempt_failed(
        self,
        *,
        record_id,
        max_attempts: int,
    ) -> bool:
        for r in self._records:
            if r.id == record_id:
                r.attempts += 1
                r.failed = r.attempts >= max_attempts
                return r.failed
        return False

    async def mark_sent(
        self,
        *,
        record_id: object,
    ) -> None:
        for r in self._records:
            if r.id == record_id:
                r.sent = True

    def all_records(self) -> list[OutboxRecord]:
        return list(self._records)


class FakeWebhookDeliveryRepository:
    """Имитация webhook_deliveries: upsert + клейм с fencing token (locked_until)."""

    def __init__(self) -> None:
        self._records: dict[str, WebhookDelivery] = {}
        self._locked_until: dict[str, datetime] = {}
        self.delivered: list[str] = []
        self.tokens: list[datetime] = []

    async def create_if_absent(
        self,
        *,
        event_id: str,
        payment_id: uuid.UUID,
    ) -> WebhookDelivery:
        if event_id not in self._records:
            self._records[event_id] = WebhookDelivery(
                event_id=event_id,
                payment_id=payment_id,
            )
        return self._records[event_id]

    async def try_claim(
        self,
        *,
        event_id: str,
        ttl_seconds: float,
    ) -> datetime | None:
        record = self._records.get(event_id)
        if record is None or record.status in (
            WebhookDeliveryStatus.DELIVERED,
            WebhookDeliveryStatus.FAILED,
        ):
            return None
        now = datetime.now(timezone.utc)
        held = self._locked_until.get(event_id)
        if held is not None and held >= now:
            return None
        token = now + timedelta(seconds=ttl_seconds)
        self._locked_until[event_id] = token
        self.tokens.append(token)
        record.status = WebhookDeliveryStatus.DELIVERING
        record.locked_until = token
        return token

    async def release_claim(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> None:
        if self._locked_until.get(event_id) != claim_token:
            return  # чужой клейм не трогаем
        self._locked_until.pop(
            event_id,
            None,
        )
        record = self._records.get(event_id)
        if record is not None:
            record.locked_until = None
            record.status = WebhookDeliveryStatus.PENDING

    async def mark_delivered(
        self,
        *,
        event_id: str,
        claim_token: datetime,
    ) -> bool:
        record = self._records.get(event_id)
        if record is None or record.status != WebhookDeliveryStatus.DELIVERING:
            return False
        if self._locked_until.get(event_id) != claim_token:
            return (
                False  # fencing: клейм устарел, состояние принадлежит новому владельцу
            )
        record.status = WebhookDeliveryStatus.DELIVERED
        record.locked_until = None
        self._locked_until.pop(
            event_id,
            None,
        )
        self.delivered.append(event_id)
        return True

    async def mark_attempt_failed(
        self,
        *,
        event_id: str,
        claim_token: datetime,
        max_attempts: int,
        error: str,
    ) -> tuple[int, bool] | None:
        record = self._records.get(event_id)
        if record is None or record.status != WebhookDeliveryStatus.DELIVERING:
            return None
        if self._locked_until.get(event_id) != claim_token:
            return None  # fencing: попытку засчитает новый владелец
        record.attempts += 1
        record.last_error = error
        record.status = (
            WebhookDeliveryStatus.FAILED
            if record.attempts >= max_attempts
            else WebhookDeliveryStatus.PENDING
        )
        record.locked_until = None
        self._locked_until.pop(
            event_id,
            None,
        )
        return record.attempts, record.status == WebhookDeliveryStatus.FAILED

    async def get(
        self,
        *,
        event_id: str,
    ) -> WebhookDelivery | None:
        return self._records.get(event_id)

    # --- helpers для тестов -------------------------------------------------
    def expire_claim(
        self,
        *,
        event_id: str,
    ) -> None:
        """Имитация SIGKILL: клейм остался, но его TTL истёк."""
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        self._locked_until[event_id] = past
        record = self._records.get(event_id)
        if record is not None:
            record.locked_until = past

    def held_claim(
        self,
        *,
        event_id: str,
    ) -> datetime | None:
        return self._locked_until.get(event_id)


class FakeUnitOfWork:
    """UoW с честной семантикой отката: снимок на входе, восстановление при ошибке."""

    def __init__(self) -> None:
        self.payments = FakePaymentRepository()
        self.outbox = FakeOutboxRepository()
        self.webhook_deliveries = FakeWebhookDeliveryRepository()
        self.committed = 0
        self.rolled_back = 0
        self.in_transaction = False
        self._snapshot: dict[str, Any] | None = None

    async def __aenter__(self) -> "FakeUnitOfWork":
        self.in_transaction = True
        self._snapshot = self._snapshot_state()
        return self

    def _snapshot_state(self) -> dict[str, Any]:
        return {
            "by_id": copy.deepcopy(self.payments._by_id),  # noqa: SLF001
            "by_key": copy.deepcopy(self.payments._by_key),  # noqa: SLF001
            "claimed": set(self.payments.claimed),
            "finalized": set(self.payments.finalized_elsewhere),
            "outbox": copy.deepcopy(self.outbox.all_records()),
            "deliveries": copy.deepcopy(
                self.webhook_deliveries._records
            ),  # noqa: SLF001
            "delivery_locks": dict(
                self.webhook_deliveries._locked_until
            ),  # noqa: SLF001
        }

    async def __aexit__(
        self,
        exc_type,
        exc,
        tb,
    ) -> None:
        self.in_transaction = False
        if exc is not None:
            self._restore()
            self.rolled_back += 1

    def _restore(self) -> None:
        if self._snapshot is None:
            return
        self.payments._by_id = self._snapshot["by_id"]  # noqa: SLF001
        self.payments._by_key = self._snapshot["by_key"]  # noqa: SLF001
        self.payments.claimed = self._snapshot["claimed"]  # noqa: SLF001
        self.payments.finalized_elsewhere = self._snapshot["finalized"]  # noqa: SLF001
        self.outbox._records = self._snapshot["outbox"]  # noqa: SLF001
        self.webhook_deliveries._records = self._snapshot["deliveries"]  # noqa: SLF001
        self.webhook_deliveries._locked_until = self._snapshot[
            "delivery_locks"
        ]  # noqa: SLF001

    async def commit(self) -> None:
        self.committed += 1
        self._snapshot = self._snapshot_state()

    async def rollback(self) -> None:
        self._restore()
        self.rolled_back += 1

    def add_payment_directly(
        self,
        *,
        payment: Payment,
    ) -> None:
        """Тестовый хелпер: положить платёж, не вызывая add()."""
        self.payments.put(payment=payment)


class FakeGateway:
    """Эмулирует шлюз: результат задаётся заранее."""

    def __init__(
        self,
        *,
        succeed: bool = True,
        reason: str | None = None,
        fail_times: int = 0,
    ) -> None:
        self.succeed = succeed
        self.reason = reason
        self.fail_times = fail_times  # первые N вызовов падают исключением
        self.calls = 0
        self.last_idempotency_key = ""

    async def process(
        self,
        *,
        payment_id: str,
        amount: object,
        currency: str,
        idempotency_key: str = "",
    ) -> GatewayResult:
        self.calls += 1
        self.last_idempotency_key = idempotency_key
        if self.calls <= self.fail_times:
            raise RuntimeError("gateway is temporarily unavailable")
        return GatewayResult(
            succeeded=self.succeed,
            reason=self.reason,
        )


class FakeWebhookSender:
    def __init__(
        self,
        *,
        fail_first: int = 0,
    ) -> None:
        self.sent: list[dict[str, Any]] = []
        self.fail_first = fail_first
        self._calls = 0

    async def send(
        self,
        *,
        url: str,
        payload: dict[str, Any],
    ) -> None:
        self._calls += 1
        if self._calls <= self.fail_first:
            raise RuntimeError("webhook endpoint unreachable")
        self.sent.append(
            {
                "url": url,
                "payload": payload,
            }
        )

    @property
    def calls(self) -> int:
        return self._calls


class FakeMessagePublisher:
    def __init__(
        self,
        *,
        fail_first: int = 0,
        poison_payload: dict | None = None,
        unavailable_first: int = 0,
    ) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.message_ids: list[str | None] = []
        self.fail_first = fail_first
        self.poison_payload = poison_payload
        self.unavailable_first = unavailable_first
        self._calls = 0

    async def publish(
        self,
        *,
        subject: str,
        payload: dict[str, Any],
        message_id: str | None = None,
    ) -> None:
        from payment.application.ports.message_publisher import PublisherUnavailable

        self._calls += 1
        if self._calls <= self.unavailable_first:
            raise PublisherUnavailable(message="broker is down")
        if self.poison_payload is not None and payload == self.poison_payload:
            raise RuntimeError("publish rejected for this payload")
        if self._calls <= self.fail_first:
            raise RuntimeError("broker is down")
        self.published.append(
            (
                subject,
                payload,
            )
        )
        self.message_ids.append(message_id)

    @property
    def calls(self) -> int:
        return self._calls


__all__ = [
    "GatewayResult",
    "FakePaymentRepository",
    "FakeOutboxRepository",
    "FakeWebhookDeliveryRepository",
    "FakeUnitOfWork",
    "FakeGateway",
    "FakeWebhookSender",
    "FakeMessagePublisher",
    "PaymentStatus",
]
