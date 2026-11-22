from __future__ import annotations

import inspect

import pytest

from payment.application.services.outbox_service import OutboxService
from payment.application.services.payment_service import PaymentService
from payment.application.services.webhook_service import WebhookService
from payment.domain.services.retry_policy import ExponentialBackoffRetryPolicy

pytestmark = [
    pytest.mark.unit,
    pytest.mark.contracts,
]


TARGETS = [
    (
        PaymentService,
        "_fingerprint",
    ),
    (
        PaymentService,
        "create_payment",
    ),
    (
        PaymentService,
        "_create_payment",
    ),
    (
        PaymentService,
        "_read_existing",
    ),
    (
        PaymentService,
        "_existing",
    ),
    (
        PaymentService,
        "get_payment",
    ),
    (
        PaymentService,
        "process_payment",
    ),
    (
        PaymentService,
        "_acquire_claim",
    ),
    (
        PaymentService,
        "_claim",
    ),
    (
        PaymentService,
        "_release_claim",
    ),
    (
        PaymentService,
        "_do_release_claim",
    ),
    (
        PaymentService,
        "_load",
    ),
    (
        PaymentService,
        "_finalize",
    ),
    (
        PaymentService,
        "_resume_result",
    ),
    (
        PaymentService,
        "_result",
    ),
    (
        WebhookService,
        "deliver",
    ),
    (
        WebhookService,
        "_result_without_claim",
    ),
    (
        WebhookService,
        "_open_task",
    ),
    (
        WebhookService,
        "_get",
    ),
    (
        WebhookService,
        "_release_claim",
    ),
    (
        WebhookService,
        "_do_release_claim",
    ),
    (
        WebhookService,
        "_mark_delivered",
    ),
    (
        WebhookService,
        "_mark_failed",
    ),
    (
        OutboxService,
        "subject_for",
    ),
    (
        OutboxService,
        "message_id_for",
    ),
    (
        ExponentialBackoffRetryPolicy,
        "delay_for_attempt",
    ),
]


def test_service_and_policy_method_arguments_are_keyword_only():
    for owner, method_name in TARGETS:
        method = getattr(
            owner,
            method_name,
        )
        parameters = list(inspect.signature(method).parameters.values())
        if parameters and parameters[0].name in {
            "self",
            "cls",
        }:
            parameters = parameters[1:]
        label = f"{owner.__name__}.{method_name}"
        assert parameters, label
        assert all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY for parameter in parameters
        ), label
