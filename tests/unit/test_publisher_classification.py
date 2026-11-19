"""Классификация ошибок публикации: явные типы, без эвристик по именам."""

from aio_pika.exceptions import (
    AMQPConnectionError,
    ChannelClosed,
    ChannelInvalidStateError,
    DeliveryError,
    PublishError,
)
from faststream.exceptions import IncorrectState

from payment.adapters.messaging.publisher import _UNAVAILABLE_TYPES as _UNAVAILABLE_TYPES_FOR_TEST
from payment.adapters.messaging.publisher import _is_unavailable
from payment.application.ports.message_publisher import PublisherUnavailable


class _Frame:
    """Заглушка frame для DeliveryError."""


class FakeConnectionFailure(Exception):
    """Имя содержит 'Connection', но тип неизвестен — эвристики больше нет."""


class TestUnavailable:
    def test_connection_refused(self) -> None:
        assert _is_unavailable(AMQPConnectionError(111, "refused"))

    def test_channel_invalid_state_after_broker_restart(self) -> None:
        assert _is_unavailable(ChannelInvalidStateError("closed"))

    def test_channel_closed(self) -> None:
        assert _is_unavailable(ChannelClosed(404, "NOT_FOUND"))

    def test_faststream_not_connected(self) -> None:
        assert _is_unavailable(IncorrectState("Broker is not connected yet"))

    def test_wrapped_unavailable(self) -> None:
        assert _is_unavailable(PublisherUnavailable("down"))

    def test_builtin_socket_errors(self) -> None:
        assert _is_unavailable(ConnectionResetError())
        assert _is_unavailable(TimeoutError())


class TestPerRecord:
    def test_delivery_error_is_not_outage(self) -> None:
        assert not _is_unavailable(DeliveryError("rejected", _Frame()))

    def test_publish_error_is_a_delivery_error(self) -> None:
        # PublishError наследует DeliveryError: отказ доставки — ошибка записи.
        # Экземпляр не создаём: aiormq требует реальный DeliveredMessage.
        assert issubclass(PublishError, DeliveryError)
        assert not issubclass(PublishError, _UNAVAILABLE_TYPES_FOR_TEST)

    def test_unknown_type_with_connection_in_name(self) -> None:
        # раньше такие ловились по подстроке и блокировали батч навсегда
        assert not _is_unavailable(FakeConnectionFailure())

    def test_value_error(self) -> None:
        assert not _is_unavailable(ValueError("bad payload"))
