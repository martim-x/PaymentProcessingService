"""Классификация сбоев обработчика: транзиентная инфраструктура vs бизнес-отказ.

Транзиентные сбои БД/транспорта получают отдельную цепочку ретраев со своим
счётчиком (`x-infra-attempt`), поэтому OperationalError не должен расходовать
business-попытки (`x-attempt`) и не должен упираться в DLQ после трёх секунд
ретраев, оставляя платёж в `pending`.
"""

import asyncio

import pytest
from sqlalchemy.exc import (
    DisconnectionError,
    IntegrityError,
    InterfaceError,
    OperationalError,
)

from payment.adapters.messaging.error_classification import (
    InfrastructureErrorClassifier,
)
from payment.application.exceptions import (
    PaymentBusy,
    PaymentNotFound,
    WebhookBusy,
    WebhookDeliveryFailed,
)
from payment.application.ports.message_publisher import PublisherUnavailable

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


def operational_error() -> OperationalError:
    return OperationalError(
        "UPDATE payments SET status='succeeded'",
        {},
        ConnectionResetError("server closed the connection unexpectedly"),
    )


class TestTransientInfrastructureFailures:
    def test_sqlalchemy_operational_error_is_transient(self) -> None:
        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(operational_error())
            is True
        )

    def test_sqlalchemy_interface_error_is_transient(self) -> None:
        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                InterfaceError(
                    "SELECT 1",
                    {},
                    Exception("bad"),
                )
            )
            is True
        )

    def test_sqlalchemy_disconnection_error_is_transient(self) -> None:
        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                DisconnectionError("connection invalidated")
            )
            is True
        )

    def test_driver_level_operational_error_is_transient(self) -> None:
        psycopg = pytest.importorskip("psycopg")

        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                psycopg.OperationalError("consuming input failed")
            )
            is True
        )

    def test_broker_connection_error_is_transient(self) -> None:
        aio_pika = pytest.importorskip("aio_pika.exceptions")

        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                aio_pika.AMQPConnectionError("connection lost")
            )
            is True
        )

    def test_publisher_unavailable_is_transient(self) -> None:
        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                PublisherUnavailable(message="broker is down")
            )
            is True
        )

    def test_connection_error_is_transient(self) -> None:
        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                ConnectionResetError("reset by peer")
            )
            is True
        )

    def test_timeout_is_transient(self) -> None:
        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                TimeoutError("pool checkout timeout")
            )
            is True
        )
        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(
                asyncio.TimeoutError("query timeout")
            )
            is True
        )

    def test_wrapped_transient_error_is_detected_through_the_cause(self) -> None:
        def boom() -> None:
            try:
                raise operational_error()
            except OperationalError as exc:
                raise RuntimeError("service failed") from exc

        with pytest.raises(RuntimeError) as caught:
            boom()

        assert (
            InfrastructureErrorClassifier.is_transient_infra_error(caught.value) is True
        )


class TestBusinessFailuresAreNotTransient:
    @pytest.mark.parametrize(
        "exc",
        [
            PaymentBusy(payment_id="payment is being processed"),
            PaymentNotFound(payment_id="p-1"),
            WebhookBusy(event_id="evt-1"),
            WebhookDeliveryFailed(
                event_id="evt-1",
                attempt=1,
                error="endpoint unreachable",
            ),
            ValueError("invalid currency"),
            KeyError("amount"),
            RuntimeError("gateway rejected"),
            ZeroDivisionError("boom"),
        ],
    )
    def test_business_errors_are_not_classified_as_infra(
        self,
        exc: BaseException,
    ) -> None:
        assert InfrastructureErrorClassifier.is_transient_infra_error(exc) is False

    def test_integrity_error_is_a_conflict_not_a_transient_failure(self) -> None:
        conflict = IntegrityError(
            "INSERT INTO payments",
            {},
            Exception("duplicate key"),
        )

        assert InfrastructureErrorClassifier.is_transient_infra_error(conflict) is False

    def test_business_error_wrapping_another_business_error_is_not_transient(
        self,
    ) -> None:
        try:
            try:
                raise PaymentNotFound(payment_id="p-1")
            except PaymentNotFound as exc:
                raise RuntimeError("wrapped") from exc
        except RuntimeError as caught:
            wrapped = caught

        assert InfrastructureErrorClassifier.is_transient_infra_error(wrapped) is False
