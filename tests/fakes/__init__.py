"""Fake-реализации портов для unit-тестов use case'ов.

Всё в одном модуле, чтобы тесты могли делать `from tests.fakes import ...`.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import PaymentId


@dataclass(frozen=True, slots=True)
class GatewayResult:
    succeeded: bool
    reason: Optional[str] = None


class FakePaymentRepository:
    def __init__(self) -> None:
        self._by_id: dict[str, Payment] = {}
        self._by_key: dict[str, Payment] = {}

    async def add(self, payment: Payment) -> None:
        self._by_id[str(payment.id)] = payment
        self._by_key[payment.idempotency_key.value] = payment

    async def save(self, payment: Payment) -> None:
        self._by_id[str(payment.id)] = payment

    async def get(self, payment_id: PaymentId) -> Optional[Payment]:
        return self._by_id.get(str(payment_id))

    async def get_by_idempotency_key(self, key: str) -> Optional[Payment]:
        return self._by_key.get(key)


class FakeOutboxRecord:
    def __init__(self, payload: dict, aggregate_id: str) -> None:
        self.id = uuid.uuid4()
        self.payload = payload
        self.aggregate_id = aggregate_id
        self.sent = False
        self.published_at: Optional[datetime] = None
        self.attempts = 0


class FakeOutboxRepository:
    def __init__(self) -> None:
        self._records: list[FakeOutboxRecord] = []

    async def add(self, *, payload: dict, aggregate_id: str) -> FakeOutboxRecord:
        record = FakeOutboxRecord(payload=payload, aggregate_id=aggregate_id)
        self._records.append(record)
        return record

    async def fetch_unsent(self, batch: int = 100) -> list[FakeOutboxRecord]:
        return [r for r in self._records if not r.sent][:batch]

    async def mark_sent(self, record_id: uuid.UUID) -> None:
        for r in self._records:
            if r.id == record_id:
                r.sent = True
                r.published_at = datetime.now(timezone.utc)

    def all_records(self) -> list[FakeOutboxRecord]:
        return list(self._records)


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.payments = FakePaymentRepository()
        self.outbox = FakeOutboxRepository()
        self.committed = 0
        self.rolled_back = 0
        self.in_transaction = False

    async def __aenter__(self) -> "FakeUnitOfWork":
        self.in_transaction = True
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.in_transaction = False
        if exc is not None:
            self.rolled_back += 1

    async def commit(self) -> None:
        self.committed += 1

    async def rollback(self) -> None:
        self.rolled_back += 1


class FakeGateway:
    """Эмулирует шлюз: результат задаётся заранее."""

    def __init__(self, *, succeed: bool = True, reason: Optional[str] = None) -> None:
        self.succeed = succeed
        self.reason = reason
        self.calls = 0

    async def process(self, *, payment_id: str, amount: Any, currency: str) -> GatewayResult:
        self.calls += 1
        return GatewayResult(succeeded=self.succeed, reason=self.reason)


class FakeWebhookSender:
    def __init__(self, *, fail_first: int = 0) -> None:
        self.sent: list[dict[str, Any]] = []
        self.fail_first = fail_first
        self._calls = 0

    async def send(self, *, url: str, payload: dict[str, Any], signature: str = "") -> None:
        self._calls += 1
        if self._calls <= self.fail_first:
            raise RuntimeError("webhook endpoint unreachable")
        self.sent.append({"url": url, "payload": payload, "signature": signature})

    @property
    def calls(self) -> int:
        return self._calls


class FakeMessagePublisher:
    def __init__(self, *, fail_first: int = 0) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.fail_first = fail_first
        self._calls = 0

    async def publish(self, *, subject: str, payload: dict[str, Any]) -> None:
        self._calls += 1
        if self._calls <= self.fail_first:
            raise RuntimeError("broker is down")
        self.published.append((subject, payload))

    @property
    def calls(self) -> int:
        return self._calls
