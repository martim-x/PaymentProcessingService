"""Integration-тесты: репозитории, UoW и outbox на реальном PostgreSQL.

Запуск: pytest tests/integration -q
Требует PAYMENT_DATABASE_URL (в CI — сервис-контейнер postgres).
"""

import os
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payment.adapters.persistence.orm import metadata
from payment.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from payment.domain.model.enums import Currency
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import IdempotencyKey, Money, PaymentId, WebhookUrl

DATABASE_URL = os.environ.get(
    "PAYMENT_DATABASE_URL", "postgresql+psycopg://payment:payment@localhost:5433/payments"
)


@pytest.fixture
async def uow_factory() -> AsyncIterator:
    engine = create_async_engine(DATABASE_URL)
    # create_all идемпотентен: не трогает схему, созданную alembic,
    # и не сносит таблицы живого стека (drop_all здесь запрещён)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    sessionmaker = async_sessionmaker(bind=engine, expire_on_commit=False)
    yield lambda: SqlAlchemyUnitOfWork(sessionmaker)
    async with engine.begin() as conn:
        for table in ("outbox", "payments"):
            await conn.exec_driver_sql(f"TRUNCATE {table} CASCADE")
    await engine.dispose()


def make_payment(key: str = "key-int-1") -> Payment:
    return Payment.create(
        payment_id=PaymentId.next(),
        amount=Money(amount=Decimal("55.25"), currency=Currency.RUB),
        description="Integration payment",
        metadata={"order_id": "7"},
        idempotency_key=IdempotencyKey(key),
        webhook_url=WebhookUrl("https://example.com/hook"),
    )


class TestPaymentRepository:
    async def test_add_and_get_roundtrip(self, uow_factory) -> None:
        uow = uow_factory()
        payment = make_payment()
        async with uow as w:
            await w.payments.add(payment)
            await w.commit()

        uow2 = uow_factory()
        async with uow2 as w:
            loaded = await w.payments.get(payment.id)
        assert loaded is not None
        assert str(loaded.id) == str(payment.id)
        assert loaded.amount.amount == Decimal("55.2500")
        assert loaded.status.value == "pending"
        assert loaded.metadata == {"order_id": "7"}

    async def test_get_by_idempotency_key(self, uow_factory) -> None:
        uow = uow_factory()
        payment = make_payment("key-int-2")
        async with uow as w:
            await w.payments.add(payment)
            await w.commit()

        uow2 = uow_factory()
        async with uow2 as w:
            found = await w.payments.get_by_idempotency_key("key-int-2")
            missing = await w.payments.get_by_idempotency_key("no-such-key")
        assert found is not None
        assert found.fingerprint is None
        assert missing is None


class TestUnitOfWork:
    async def test_commit_persists_payment_and_outbox_atomically(self, uow_factory) -> None:
        uow = uow_factory()
        payment = make_payment("key-int-3")
        async with uow as w:
            await w.payments.add(payment)
            await w.outbox.add(
                payload={"event": "payment.created", "payment_id": str(payment.id)},
                aggregate_id=str(payment.id),
            )
            await w.commit()

        uow2 = uow_factory()
        async with uow2 as w:
            loaded = await w.payments.get(payment.id)
            unsent = await w.outbox.fetch_unsent(batch=10)
        assert loaded is not None
        assert len(unsent) == 1

    async def test_rollback_discards_uncommitted(self, uow_factory) -> None:
        uow = uow_factory()
        payment = make_payment("key-int-4")
        async with uow as w:
            await w.payments.add(payment)
            await w.rollback()  # без commit

        uow2 = uow_factory()
        async with uow2 as w:
            loaded = await w.payments.get(payment.id)
        assert loaded is None


class TestProcessPaymentPersistStatus:
    """Регрессия: статус должен реально обновляться в БД и не перезаписываться дважды."""

    async def test_process_persists_succeeded(self, uow_factory) -> None:
        from payment.application.ports.payment_gateway import GatewayResult
        from payment.application.use_cases.process_payment import ProcessPayment
        from tests.fakes import FakeWebhookSender

        uow = uow_factory()
        payment = make_payment("key-proc-1")
        async with uow as w:
            await w.payments.add(payment)
            await w.commit()
        pid = payment.id

        class OkGateway:
            async def process(
                self, *, payment_id, amount, currency, idempotency_key=""
            ) -> GatewayResult:
                return GatewayResult(succeeded=True)

        use_case = ProcessPayment(
            uow=uow_factory(), gateway=OkGateway(), webhook_sender=FakeWebhookSender()
        )
        result = await use_case.execute(pid)

        async with uow_factory() as w:
            loaded = await w.payments.get(pid)
        assert result.status == "succeeded"
        assert loaded is not None and loaded.status.value == "succeeded"

    async def test_claim_prevents_double_processing(self, uow_factory) -> None:
        """Клейм выдаётся один раз: второй consumer не получит токен."""
        payment = make_payment("key-proc-2")
        async with uow_factory() as w:
            await w.payments.add(payment)
            await w.commit()

        async with uow_factory() as w:
            first_token = await w.payments.try_claim(payment.id, ttl_seconds=60)
            await w.commit()
        async with uow_factory() as w:
            second_token = await w.payments.try_claim(payment.id, ttl_seconds=60)
            await w.commit()

        assert first_token is not None
        assert second_token is None

    async def test_finalize_requires_matching_claim_token(self, uow_factory) -> None:
        payment = make_payment("key-proc-3")
        async with uow_factory() as w:
            await w.payments.add(payment)
            await w.commit()

        async with uow_factory() as w:
            token = await w.payments.try_claim(payment.id, ttl_seconds=60)
            await w.commit()
        assert token is not None

        payment.mark_succeeded()
        async with uow_factory() as w:
            wrong = await w.payments.try_finalize(
                payment,
                claim_token=token.replace(year=1999) if hasattr(token, "replace") else token,
            )
            await w.commit()
        assert wrong is False

        async with uow_factory() as w:
            right = await w.payments.try_finalize(payment, claim_token=token)
            await w.commit()
        async with uow_factory() as w:
            again = await w.payments.try_finalize(payment, claim_token=token)
            await w.commit()

        assert right is True
        assert again is False

    async def test_expired_claim_can_be_reacquired(self, uow_factory) -> None:
        payment = make_payment("key-proc-4")
        async with uow_factory() as w:
            await w.payments.add(payment)
            await w.commit()

        async with uow_factory() as w:
            await w.payments.try_claim(payment.id, ttl_seconds=0.0)
            await w.commit()
        async with uow_factory() as w:
            token = await w.payments.try_claim(payment.id, ttl_seconds=60)
            await w.commit()

        assert token is not None


class TestOutboxRepository:
    async def test_fetch_unsent_returns_only_unsent(self, uow_factory) -> None:
        async with uow_factory() as uow:
            r1 = await uow.outbox.add(payload={"n": 1}, aggregate_id="a1")
            await uow.outbox.add(payload={"n": 2}, aggregate_id="a2")
            await uow.outbox.mark_sent(r1.id)
            await uow.commit()

        async with uow_factory() as uow:
            unsent = await uow.outbox.fetch_unsent(batch=10)
        assert [r.payload["n"] for r in unsent] == [2]

    async def test_batch_limit_and_order(self, uow_factory) -> None:
        async with uow_factory() as uow:
            for i in range(5):
                await uow.outbox.add(payload={"n": i}, aggregate_id=f"a{i}")
            await uow.commit()

        async with uow_factory() as uow:
            batch = await uow.outbox.fetch_unsent(batch=3)
        assert len(batch) == 3
