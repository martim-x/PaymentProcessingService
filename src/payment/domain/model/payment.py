"""Агрегат Payment (Aggregate Root)."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Union

from payment.domain.events import PaymentCreated, PaymentFailed, PaymentSucceeded
from payment.domain.exceptions import InvalidStatusTransition
from payment.domain.model.enums import PaymentStatus
from payment.domain.model.value_objects import IdempotencyKey, Money, PaymentId, WebhookUrl

_DomainEvent = Union[PaymentCreated, PaymentSucceeded, PaymentFailed]


@dataclass(slots=True)
class Payment:
    """Корень агрегата: владеет статусом и переходами pending -> succeeded|failed.

    Dataclass не frozen, поэтому изоляция полей — соглашение, а не гарантия
    языка: снаружи статус меняют через методы агрегата (они проверяют переход
    и пишут событие), прямое присваивание допустимо только при маппинге из БД.
    Доменные события копятся до collect_events.
    """

    id: PaymentId
    amount: Money
    description: str
    metadata: dict[str, Any]
    idempotency_key: IdempotencyKey
    webhook_url: WebhookUrl
    status: PaymentStatus = PaymentStatus.PENDING
    fingerprint: Optional[str] = None  # sha256 тела исходного запроса (для 409 по idempotency)
    failure_reason: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processed_at: Optional[datetime] = None
    # внутренний клейм обработки: не отдаётся в API, защищает от двойного списания
    locked_until: Optional[datetime] = None
    _events: list[_DomainEvent] = field(default_factory=list, repr=False, compare=False)

    @classmethod
    def create(
        cls,
        *,
        payment_id: PaymentId,
        amount: Money,
        description: str,
        metadata: dict[str, Any],
        idempotency_key: IdempotencyKey,
        webhook_url: WebhookUrl,
        fingerprint: Optional[str] = None,
    ) -> "Payment":
        """Фабрика создания нового платежа: статус pending + событие PaymentCreated."""
        payment = cls(
            id=payment_id,
            amount=amount,
            description=description,
            metadata=metadata,
            idempotency_key=idempotency_key,
            webhook_url=webhook_url,
            fingerprint=fingerprint,
        )
        payment._events.append(
            PaymentCreated(
                payment_id=payment.id.value,
                amount=payment.amount.amount,
                currency=payment.amount.currency,
                idempotency_key=payment.idempotency_key.value,
                webhook_url=payment.webhook_url.value,
                metadata=dict(payment.metadata),
            )
        )
        return payment

    def mark_succeeded(self, *, processed_at: Optional[datetime] = None) -> None:
        """pending -> succeeded и событие PaymentSucceeded.

        InvalidStatusTransition, если платёж уже финализирован: повторная
        обработка не должна переписывать исход.
        """
        if self.status is not PaymentStatus.PENDING:
            raise InvalidStatusTransition(self.status.value, PaymentStatus.SUCCEEDED.value)
        self.status = PaymentStatus.SUCCEEDED
        self.processed_at = processed_at or datetime.now(timezone.utc)
        self._events.append(
            PaymentSucceeded(payment_id=self.id.value, processed_at=self.processed_at)
        )

    def mark_failed(self, *, reason: str, processed_at: Optional[datetime] = None) -> None:
        """pending -> failed с причиной и событием PaymentFailed.

        InvalidStatusTransition при повторном вызове: исход платежа неизменен.
        """
        if self.status is not PaymentStatus.PENDING:
            raise InvalidStatusTransition(self.status.value, PaymentStatus.FAILED.value)
        self.status = PaymentStatus.FAILED
        self.failure_reason = reason
        self.processed_at = processed_at or datetime.now(timezone.utc)
        self._events.append(
            PaymentFailed(payment_id=self.id.value, reason=reason, processed_at=self.processed_at)
        )

    def collect_events(self) -> list[_DomainEvent]:
        """Отдаёт накопленные события и очищает буфер: повторный вызов вернёт пусто."""
        events, self._events = self._events, []
        return events

    @property
    def is_terminal(self) -> bool:
        """Платёж финализирован (не pending) и повторной обработке не подлежит."""
        return self.status is not PaymentStatus.PENDING

    def to_public_dict(self) -> dict[str, Any]:
        """Плоское представление для API/брокера (без доменных объектов).

        idempotency_key, fingerprint и locked_until не попадают в словарь:
        это внутреннее состояние, а не данные клиента.
        """
        return {
            "id": str(self.id),
            "amount": str(self.amount.amount),
            "currency": self.amount.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "status": self.status.value,
            "failure_reason": self.failure_reason,
            "webhook_url": self.webhook_url.value,
            "created_at": self.created_at.isoformat(),
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        }
