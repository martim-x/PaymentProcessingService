"""Классификация ошибок публикации: явные типы, без эвристик по именам."""

from typing import cast

import pytest
from aio_pika.exceptions import (
    AMQPConnectionError,
    ChannelClosed,
    ChannelInvalidStateError,
    DeliveryError,
    PublishError,
)
from aiormq.abc import DeliveredMessage
from faststream.exceptions import IncorrectState
from pamqp.base import Frame

from payment.adapters.messaging.publisher import FastStreamPublisher
from payment.application.ports.message_publisher import PublisherUnavailable

_UNAVAILABLE_TYPES_FOR_TEST = FastStreamPublisher._UNAVAILABLE_TYPES

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


def _is_unavailable(exc: BaseException) -> bool:
    return FastStreamPublisher._is_unavailable(exc)


class _Frame:
    """Заглушка frame для DeliveryError."""


class FakeConnectionFailure(Exception):
    """Имя содержит 'Connection', но тип неизвестен — эвристики больше нет."""


class TestUnavailable:
    def test_connection_refused(self) -> None:
        assert _is_unavailable(
            AMQPConnectionError(
                111,
                "refused",
            )
        )

    def test_channel_invalid_state_after_broker_restart(self) -> None:
        assert _is_unavailable(ChannelInvalidStateError("closed"))

    def test_channel_closed(self) -> None:
        assert _is_unavailable(
            ChannelClosed(
                404,
                "NOT_FOUND",
            )
        )

    def test_faststream_not_connected(self) -> None:
        assert _is_unavailable(IncorrectState("Broker is not connected yet"))

    def test_wrapped_unavailable(self) -> None:
        assert _is_unavailable(PublisherUnavailable(message="down"))

    def test_builtin_socket_errors(self) -> None:
        assert _is_unavailable(ConnectionResetError())
        assert _is_unavailable(TimeoutError())


class TestPerRecord:
    def test_delivery_error_is_not_outage(self) -> None:
        assert not _is_unavailable(
            DeliveryError(
                cast(
                    DeliveredMessage,
                    object(),
                ),
                cast(
                    Frame,
                    _Frame(),
                ),
            )
        )

    def test_publish_error_is_a_delivery_error(self) -> None:
        # PublishError наследует DeliveryError: отказ доставки — ошибка записи.
        # Экземпляр не создаём: aiormq требует реальный DeliveredMessage.
        assert issubclass(
            PublishError,
            DeliveryError,
        )
        assert not issubclass(
            PublishError,
            _UNAVAILABLE_TYPES_FOR_TEST,
        )

    def test_unknown_type_with_connection_in_name(self) -> None:
        # раньше такие ловились по подстроке и блокировали батч навсегда
        assert not _is_unavailable(FakeConnectionFailure())

    def test_value_error(self) -> None:
        assert not _is_unavailable(ValueError("bad payload"))
