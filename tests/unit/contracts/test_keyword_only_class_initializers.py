from __future__ import annotations

import ast
from pathlib import Path

import pytest

from payment.adapters.messaging.publisher import PublisherUnavailable, PublishRejected
from payment.adapters.webhook.guard import UnsafeWebhookUrl
from payment.application.exceptions import (
    IdempotencyConflict,
    IdempotencyViolation,
    PaymentBusy,
    PaymentNotFound,
    WebhookBusy,
    WebhookDeliveryExhausted,
    WebhookDeliveryFailed,
)
from payment.domain.exceptions import (
    InvalidMoney,
    InvalidStatusTransition,
    InvalidWebhookUrl,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.contracts,
]


ROOT = Path(__file__).resolve().parents[3]
EXCEPTION_TYPES = {
    "PaymentBusy": PaymentBusy,
    "IdempotencyViolation": IdempotencyViolation,
    "PaymentNotFound": PaymentNotFound,
    "IdempotencyConflict": IdempotencyConflict,
    "WebhookBusy": WebhookBusy,
    "WebhookDeliveryFailed": WebhookDeliveryFailed,
    "WebhookDeliveryExhausted": WebhookDeliveryExhausted,
    "InvalidMoney": InvalidMoney,
    "InvalidStatusTransition": InvalidStatusTransition,
    "InvalidWebhookUrl": InvalidWebhookUrl,
    "PublisherUnavailable": PublisherUnavailable,
    "PublishRejected": PublishRejected,
    "UnsafeWebhookUrl": UnsafeWebhookUrl,
}


def _project_initializers() -> (
    list[tuple[str, str, ast.FunctionDef | ast.AsyncFunctionDef]]
):
    found = []
    for base in (
        ROOT / "src/payment",
        ROOT / "tests/fakes",
        ROOT / "tests/unit",
    ):
        for path in base.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            tree = ast.parse(
                path.read_text(),
                filename=str(path),
            )
            for node in ast.walk(tree):
                if not isinstance(
                    node,
                    ast.ClassDef,
                ):
                    continue
                for member in node.body:
                    if (
                        isinstance(
                            member,
                            (
                                ast.FunctionDef,
                                ast.AsyncFunctionDef,
                            ),
                        )
                        and member.name == "__init__"
                    ):
                        if path.is_relative_to(
                            ROOT / "tests/unit"
                        ) and node.name.startswith("Test"):
                            continue
                        positional = (
                            member.args.args[1:]
                            if member.args.args
                            and member.args.args[0].arg
                            in {
                                "self",
                                "cls",
                            }
                            else member.args.args
                        )
                        if positional:
                            found.append(
                                (
                                    str(path.relative_to(ROOT)),
                                    node.name,
                                    member,
                                )
                            )
    return found


def test_project_owned_explicit_initializers_are_keyword_only() -> None:
    violations = []
    for path, cls, init in _project_initializers():
        positional = (
            init.args.args[1:]
            if init.args.args
            and init.args.args[0].arg
            in {
                "self",
                "cls",
            }
            else init.args.args
        )
        if positional:
            violations.append(
                f"{path}:{init.lineno} {cls}.__init__ still accepts positional parameters"
            )
    assert not violations, "\n".join(violations)


EXCEPTION_CASES = [
    (
        "PaymentBusy",
        {"payment_id": "p-1"},
        ("p-1",),
    ),
    (
        "IdempotencyViolation",
        {"idempotency_key": "key-1"},
        ("key-1",),
    ),
    (
        "PaymentNotFound",
        {"payment_id": "p-1"},
        ("Payment p-1 not found",),
    ),
    (
        "IdempotencyConflict",
        {"idempotency_key": "key-1"},
        ("Idempotency-Key 'key-1' was already used with a different payload",),
    ),
    (
        "WebhookBusy",
        {"event_id": "event-1"},
        ("Webhook event event-1 is being delivered by another consumer",),
    ),
    (
        "WebhookDeliveryFailed",
        {
            "event_id": "event-1",
            "attempt": 2,
            "error": "timeout",
        },
        ("Webhook event event-1 attempt 2 failed: timeout",),
    ),
    (
        "WebhookDeliveryExhausted",
        {
            "event_id": "event-1",
            "attempts": 3,
            "error": "timeout",
        },
        ("Webhook event event-1 failed after 3 attempt(s): timeout",),
    ),
    (
        "InvalidMoney",
        {
            "amount": "bad",
            "reason": "reason",
        },
        ("Invalid money: reason (amount='bad')",),
    ),
    (
        "InvalidStatusTransition",
        {
            "current": "pending",
            "target": "failed",
        },
        ("Cannot transition payment from 'pending' to 'failed'",),
    ),
    (
        "InvalidWebhookUrl",
        {"url": "url"},
        ("Invalid webhook url: 'url'",),
    ),
    (
        "PublisherUnavailable",
        {"message": "offline"},
        ("offline",),
    ),
    (
        "PublishRejected",
        {"message": "nack"},
        ("nack",),
    ),
    (
        "UnsafeWebhookUrl",
        {"message": "private"},
        ("private",),
    ),
]


def test_exception_messages_and_args_are_preserved() -> None:
    for name, kwargs, expected in EXCEPTION_CASES:
        exc = EXCEPTION_TYPES[name](**kwargs)
        assert exc.args == expected, name
        assert str(exc) == expected[0], name


def test_all_custom_exception_constructor_params_are_keyword_only() -> None:
    import inspect

    for exception in EXCEPTION_TYPES.values():
        parameters = list(inspect.signature(exception).parameters.values())
        assert parameters
        assert all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY for parameter in parameters
        ), exception.__name__
