import inspect
from dataclasses import fields

import pytest

from payment.adapters.messaging.routing import FailureRoute
from payment.application.dto import (
    CreatePaymentCommand,
    CreatePaymentResult,
    PaymentDetails,
    ProcessPaymentResult,
    WebhookDeliveryCommand,
)
from payment.application.ports.payment_gateway import GatewayResult
from payment.application.ports.repositories import OutboxRecord, WebhookDelivery
from payment.application.services.webhook_service import WebhookDeliveryResult
from payment.domain.events import PaymentCreated, PaymentFailed, PaymentSucceeded
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import (
    IdempotencyKey,
    Money,
    PaymentId,
    WebhookUrl,
)
from payment.domain.policies.retry_policy import ExponentialBackoffRetryPolicy
from tests.fakes import GatewayResult as FakeGatewayResult

pytestmark = [
    pytest.mark.unit,
    pytest.mark.contracts,
]


DATACLASSES = [
    FailureRoute,
    WebhookDeliveryCommand,
    CreatePaymentCommand,
    CreatePaymentResult,
    ProcessPaymentResult,
    PaymentDetails,
    GatewayResult,
    OutboxRecord,
    WebhookDelivery,
    WebhookDeliveryResult,
    PaymentCreated,
    PaymentSucceeded,
    PaymentFailed,
    Payment,
    Money,
    PaymentId,
    IdempotencyKey,
    WebhookUrl,
    ExponentialBackoffRetryPolicy,
    FakeGatewayResult,
]


def test_dataclass_and_factory_signatures_are_keyword_only():
    for cls in DATACLASSES:
        expected = tuple(field.name for field in fields(cls) if field.init)
        assert_keyword_only_signature(
            inspect.signature(cls),
            expected,
        )
        assert cls.__name__

    factories = (
        (
            PaymentId.from_string,
            ("raw",),
        ),
        (
            PaymentDetails.from_payment,
            ("payment",),
        ),
    )
    for factory, expected in factories:
        assert_keyword_only_signature(
            inspect.signature(factory),
            expected,
        )
        assert factory.__qualname__


def test_signature_contract_rejects_empty_signature_for_expected_field():
    with pytest.raises(AssertionError):
        assert_keyword_only_signature(
            inspect.Signature(),
            ("required_field",),
        )


def test_signature_contract_rejects_extra_factory_parameter():
    signature = inspect.Signature(
        [
            inspect.Parameter(
                "payment",
                inspect.Parameter.KEYWORD_ONLY,
            ),
            inspect.Parameter(
                "extra",
                inspect.Parameter.KEYWORD_ONLY,
            ),
        ]
    )
    with pytest.raises(AssertionError):
        assert_keyword_only_signature(
            signature,
            ("payment",),
        )


def assert_keyword_only_signature(
    signature,
    expected_names,
):
    """Require exact parameter names and keyword-only kinds."""
    params = tuple(signature.parameters.values())
    assert tuple(parameter.name for parameter in params) == expected_names
    assert all(parameter.kind is inspect.Parameter.KEYWORD_ONLY for parameter in params)
