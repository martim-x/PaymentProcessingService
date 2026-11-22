"""Repository port, adapter, and fake methods accept keyword-only arguments."""

import inspect

import pytest

from payment.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from payment.adapters.persistence.repositories.payment import (
    SqlAlchemyPaymentRepository,
)
from payment.adapters.persistence.repositories.webhook import (
    SqlAlchemyWebhookDeliveryRepository,
)
from payment.application.ports.repositories import (
    OutboxRepository,
    PaymentRepository,
    WebhookDeliveryRepository,
)
from tests.fakes import (
    FakeOutboxRepository,
    FakePaymentRepository,
    FakeUnitOfWork,
    FakeWebhookDeliveryRepository,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.contracts,
]


REPOSITORY_METHODS = [
    (
        PaymentRepository,
        "add",
        ["payment"],
    ),
    (
        PaymentRepository,
        "try_claim",
        [
            "payment_id",
            "ttl_seconds",
        ],
    ),
    (
        PaymentRepository,
        "release_claim",
        [
            "payment_id",
            "claim_token",
        ],
    ),
    (
        PaymentRepository,
        "try_finalize",
        [
            "payment",
            "claim_token",
        ],
    ),
    (
        PaymentRepository,
        "get",
        ["payment_id"],
    ),
    (
        PaymentRepository,
        "get_by_idempotency_key",
        ["key"],
    ),
    (
        OutboxRepository,
        "fetch_unsent",
        ["batch"],
    ),
    (
        OutboxRepository,
        "mark_sent",
        ["record_id"],
    ),
    (
        OutboxRepository,
        "mark_attempt_failed",
        [
            "record_id",
            "max_attempts",
        ],
    ),
    (
        WebhookDeliveryRepository,
        "try_claim",
        [
            "event_id",
            "ttl_seconds",
        ],
    ),
    (
        WebhookDeliveryRepository,
        "release_claim",
        [
            "event_id",
            "claim_token",
        ],
    ),
    (
        WebhookDeliveryRepository,
        "mark_delivered",
        [
            "event_id",
            "claim_token",
        ],
    ),
    (
        WebhookDeliveryRepository,
        "mark_attempt_failed",
        [
            "event_id",
            "claim_token",
            "max_attempts",
            "error",
        ],
    ),
    (
        WebhookDeliveryRepository,
        "get",
        ["event_id"],
    ),
    (
        SqlAlchemyPaymentRepository,
        "add",
        ["payment"],
    ),
    (
        SqlAlchemyPaymentRepository,
        "try_claim",
        [
            "payment_id",
            "ttl_seconds",
        ],
    ),
    (
        SqlAlchemyPaymentRepository,
        "release_claim",
        [
            "payment_id",
            "claim_token",
        ],
    ),
    (
        SqlAlchemyPaymentRepository,
        "try_finalize",
        [
            "payment",
            "claim_token",
        ],
    ),
    (
        SqlAlchemyPaymentRepository,
        "get",
        ["payment_id"],
    ),
    (
        SqlAlchemyPaymentRepository,
        "get_by_idempotency_key",
        ["key"],
    ),
    (
        SqlAlchemyOutboxRepository,
        "fetch_unsent",
        ["batch"],
    ),
    (
        SqlAlchemyOutboxRepository,
        "mark_sent",
        ["record_id"],
    ),
    (
        SqlAlchemyOutboxRepository,
        "mark_attempt_failed",
        [
            "record_id",
            "max_attempts",
        ],
    ),
    (
        SqlAlchemyWebhookDeliveryRepository,
        "try_claim",
        [
            "event_id",
            "ttl_seconds",
        ],
    ),
    (
        SqlAlchemyWebhookDeliveryRepository,
        "release_claim",
        [
            "event_id",
            "claim_token",
        ],
    ),
    (
        SqlAlchemyWebhookDeliveryRepository,
        "mark_delivered",
        [
            "event_id",
            "claim_token",
        ],
    ),
    (
        SqlAlchemyWebhookDeliveryRepository,
        "mark_attempt_failed",
        [
            "event_id",
            "claim_token",
            "max_attempts",
            "error",
        ],
    ),
    (
        SqlAlchemyWebhookDeliveryRepository,
        "get",
        ["event_id"],
    ),
    (
        FakePaymentRepository,
        "add",
        ["payment"],
    ),
    (
        FakePaymentRepository,
        "try_claim",
        [
            "payment_id",
            "ttl_seconds",
        ],
    ),
    (
        FakePaymentRepository,
        "release_claim",
        [
            "payment_id",
            "claim_token",
        ],
    ),
    (
        FakePaymentRepository,
        "try_finalize",
        [
            "payment",
            "claim_token",
        ],
    ),
    (
        FakePaymentRepository,
        "get",
        ["payment_id"],
    ),
    (
        FakePaymentRepository,
        "get_by_idempotency_key",
        ["key"],
    ),
    (
        FakeOutboxRepository,
        "fetch_unsent",
        ["batch"],
    ),
    (
        FakeOutboxRepository,
        "mark_sent",
        ["record_id"],
    ),
    (
        FakeOutboxRepository,
        "mark_attempt_failed",
        [
            "record_id",
            "max_attempts",
        ],
    ),
    (
        FakeWebhookDeliveryRepository,
        "try_claim",
        [
            "event_id",
            "ttl_seconds",
        ],
    ),
    (
        FakeWebhookDeliveryRepository,
        "release_claim",
        [
            "event_id",
            "claim_token",
        ],
    ),
    (
        FakeWebhookDeliveryRepository,
        "mark_delivered",
        [
            "event_id",
            "claim_token",
        ],
    ),
    (
        FakeWebhookDeliveryRepository,
        "mark_attempt_failed",
        [
            "event_id",
            "claim_token",
            "max_attempts",
            "error",
        ],
    ),
    (
        FakeWebhookDeliveryRepository,
        "get",
        ["event_id"],
    ),
]


def test_repository_methods_are_keyword_only():
    for owner, method, expected in REPOSITORY_METHODS:
        signature = inspect.signature(
            getattr(
                owner,
                method,
            )
        )
        assert tuple(signature.parameters) == (
            "self",
            *expected,
        ), f"{owner.__name__}.{method}"
        assert all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in list(signature.parameters.values())[1:]
        ), f"{owner.__name__}.{method}"


FAKE_HELPERS = [
    (
        FakePaymentRepository,
        "put",
        ["payment"],
    ),
    (
        FakeWebhookDeliveryRepository,
        "expire_claim",
        ["event_id"],
    ),
    (
        FakeWebhookDeliveryRepository,
        "held_claim",
        ["event_id"],
    ),
    (
        FakeUnitOfWork,
        "add_payment_directly",
        ["payment"],
    ),
]


def test_fake_repository_helpers_are_keyword_only():
    for owner, method, expected in FAKE_HELPERS:
        signature = inspect.signature(
            getattr(
                owner,
                method,
            )
        )
        assert tuple(signature.parameters) == (
            "self",
            *expected,
        ), f"{owner.__name__}.{method}"
        assert all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in list(signature.parameters.values())[1:]
        ), f"{owner.__name__}.{method}"
