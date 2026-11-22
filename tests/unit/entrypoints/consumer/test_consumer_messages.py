"""Malformed and semantically invalid messages are quarantined to the DLQ."""

import json
from typing import cast
from uuid import uuid4

import pytest
from faststream.rabbit import RabbitBroker
from faststream.rabbit.message import RabbitMessage

from payment.entrypoints.consumer.app import parse_or_quarantine_message

pytestmark = [
    pytest.mark.unit,
    pytest.mark.entrypoints,
]


class FakeMessage:
    def __init__(
        self,
        *,
        body: object,
    ) -> None:
        self.body = body
        self.acked = False

    async def ack(self) -> None:
        self.acked = True


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[tuple[object, str]] = []

    async def publish(
        self,
        payload: object,
        *,
        queue: str,
        **kwargs: object,
    ) -> bool:
        self.published.append(
            (
                payload,
                queue,
            )
        )
        return True


@pytest.mark.parametrize(
    "body",
    [
        b"{not-json",
        json.dumps(
            {
                "event": "payment.created",
                "payment_id": "not-a-uuid",
            }
        ).encode(),
        json.dumps({"event": "payment.created"}).encode(),
    ],
)
async def test_invalid_message_is_sent_to_dlq_and_acked(body: bytes) -> None:
    message = FakeMessage(body=body)
    broker = FakeBroker()

    parsed = await parse_or_quarantine_message(
        cast(
            RabbitMessage,
            message,
        ),
        cast(
            RabbitBroker,
            broker,
        ),
        "payments.dlq",
    )

    assert parsed is None
    assert broker.published == [
        (
            body,
            "payments.dlq",
        )
    ]
    assert message.acked


async def test_valid_message_returns_event_and_payment_id() -> None:
    payment_id = str(uuid4())
    body = json.dumps(
        {
            "event": "payment.created",
            "payment_id": payment_id,
        }
    ).encode()
    message = FakeMessage(body=body)
    broker = FakeBroker()

    parsed = await parse_or_quarantine_message(
        cast(
            RabbitMessage,
            message,
        ),
        cast(
            RabbitBroker,
            broker,
        ),
        "payments.dlq",
    )

    assert parsed is not None
    event, parsed_id, payload = parsed
    assert event.payment_id == payment_id
    assert str(parsed_id) == payment_id
    assert payload == {
        "event": "payment.created",
        "payment_id": payment_id,
    }
    assert broker.published == []
    assert not message.acked
