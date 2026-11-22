"""Домен: агрегат Payment, переходы статусов, доменные события."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from payment.domain.events import PaymentCreated, PaymentFailed, PaymentSucceeded
from payment.domain.exceptions import InvalidStatusTransition
from payment.domain.model.enums import Currency, PaymentStatus
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import (
    IdempotencyKey,
    Money,
    PaymentId,
    WebhookUrl,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.domain,
]


def make_money() -> Money:
    return Money(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
    )


def make_payment(**kwargs: Any) -> dict[str, Any]:
    defaults = dict(
        payment_id=PaymentId.next(),
        amount=make_money(),
        description="Test payment",
        metadata={"order_id": "42"},
        idempotency_key=IdempotencyKey(value="key-1"),
        webhook_url=WebhookUrl(value="https://example.com/hook"),
    )
    defaults.update(kwargs)
    return defaults


class TestPaymentCreate:
    def test_new_payment_is_pending(self) -> None:
        payment = Payment.create(**make_payment())

        assert payment.status is PaymentStatus.PENDING
        assert payment.processed_at is None
        assert payment.failure_reason is None

    def test_create_collects_payment_created_event(self) -> None:
        payment = Payment.create(**make_payment())

        events = payment.collect_events()
        assert len(events) == 1
        assert isinstance(
            events[0],
            PaymentCreated,
        )
        assert events[0].payment_id == payment.id.value
        # повторный вызов — события уже забраны
        assert payment.collect_events() == []

    def test_created_at_is_utc(self) -> None:
        payment = Payment.create(**make_payment())

        assert payment.created_at.tzinfo is not None
        assert payment.created_at <= datetime.now(timezone.utc)


class TestStatusTransitions:
    def test_pending_to_succeeded(self) -> None:
        payment = Payment.create(**make_payment())
        payment.mark_succeeded(processed_at=datetime.now(timezone.utc))

        assert payment.status is PaymentStatus.SUCCEEDED
        assert payment.processed_at is not None

    def test_succeeded_emits_event(self) -> None:
        payment = Payment.create(**make_payment())
        payment.collect_events()  # осушаем PaymentCreated
        payment.mark_succeeded(processed_at=datetime.now(timezone.utc))

        events = payment.collect_events()
        assert [type(e) for e in events] == [PaymentSucceeded]

    def test_pending_to_failed(self) -> None:
        payment = Payment.create(**make_payment())
        payment.collect_events()  # осушаем PaymentCreated
        payment.mark_failed(
            reason="gateway said no",
            processed_at=datetime.now(timezone.utc),
        )

        assert payment.status is PaymentStatus.FAILED
        assert payment.failure_reason == "gateway said no"
        events = payment.collect_events()
        assert [type(e) for e in events] == [PaymentFailed]

    def test_terminal_state_cannot_change(self) -> None:
        payment = Payment.create(**make_payment())
        payment.mark_succeeded(processed_at=datetime.now(timezone.utc))

        with pytest.raises(InvalidStatusTransition):
            payment.mark_failed(
                reason="late",
                processed_at=datetime.now(timezone.utc),
            )

        with pytest.raises(InvalidStatusTransition):
            payment.mark_succeeded(processed_at=datetime.now(timezone.utc))
