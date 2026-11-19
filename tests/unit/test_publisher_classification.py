"""Классификация ошибок публикации: недоступность брокера vs отказ записи."""

from aio_pika.exceptions import AMQPConnectionError, ChannelInvalidStateError

from payment.adapters.messaging.publisher import _is_unavailable
from payment.application.ports.message_publisher import PublisherUnavailable


class DeliveryError(Exception):
    """Имитация aiormq.DeliveryError по имени класса (у него нестандартный __init__)."""


def test_connection_errors_are_unavailable() -> None:
    assert _is_unavailable(AMQPConnectionError(111, "refused"))
    assert _is_unavailable(ChannelInvalidStateError("closed"))
    assert _is_unavailable(ConnectionResetError())
    assert _is_unavailable(TimeoutError())


def test_wrapped_unavailable_passes() -> None:
    assert _is_unavailable(PublisherUnavailable("down"))


def test_delivery_error_is_per_record() -> None:
    # отказ доставки конкретного сообщения не должен обнулять попытки соседних
    assert not _is_unavailable(DeliveryError("rejected by queue"))


def test_value_error_is_per_record() -> None:
    assert not _is_unavailable(ValueError("bad payload"))
