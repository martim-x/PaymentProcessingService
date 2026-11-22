"""Integration-тесты: репозитории, UoW и outbox на реальном PostgreSQL.

Запуск:
    PAYMENT_TEST_DATABASE_URL=postgresql+psycopg://user:pass@host:5432/db \\
        pytest tests/integration -q

Изоляция и безопасность:
  * URL берётся ТОЛЬКО из PAYMENT_TEST_DATABASE_URL; PAYMENT_DATABASE_URL
    рабочего стека не подхватывается как fallback (без явной переменной — skip).
  * Каждый тест работает в собственной временной схеме `pytest_<12 hex>`:
    search_path сессии содержит только эту схему, без fallback на public.
  * teardown — `DROP SCHEMA IF EXISTS "<своя схема>" CASCADE`; имя проверяется
    регуляркой, чужая схема (public, payments, ...) не удаляется никогда.
  * TRUNCATE и DROP TABLE не используются: данные в public не затрагиваются,
    даже если DSN указывает на рабочую БД.
"""

import os
from collections.abc import AsyncIterator, Callable
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payment.adapters.persistence.orm import metadata
from payment.adapters.persistence.unit_of_work import SqlAlchemyUnitOfWork
from payment.domain.model.enums import Currency
from payment.domain.model.payment import Payment
from payment.domain.model.value_objects import (
    IdempotencyKey,
    Money,
    PaymentId,
    WebhookUrl,
)
from tests.helpers.integration_db_isolation import (
    APP_DATABASE_URL_ENV,
    TEST_DATABASE_URL_ENV,
    UnsafeTestDatabaseError,
    drop_own_schema,
    new_schema_name,
    resolve_test_database_url,
)

pytestmark = pytest.mark.skipif(
    not (os.environ.get(TEST_DATABASE_URL_ENV) or "").strip(),
    reason=(
        f"{TEST_DATABASE_URL_ENV} не задан: integration-тесты не запускаются "
        f"на рабочей БД ({APP_DATABASE_URL_ENV})"
    ),
)

UowFactory = Callable[[], SqlAlchemyUnitOfWork]


@pytest.fixture
async def uow_factory() -> AsyncIterator[UowFactory]:
    url = resolve_test_database_url(os.environ)
    schema = new_schema_name()

    engine = create_async_engine(
        url,
        pool_pre_ping=True,
        # Без public отсутствующая таблица не попадёт в чужую схему.
        connect_args={"options": f"-csearch_path={schema}"},
    )
    created = False
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            created = True
            # проверка ДО любого DDL: если драйвер проигнорировал `options`,
            # create_all создал бы таблицы в public — этого делать нельзя
            search_path = await conn.scalar(
                sa.text("SELECT current_setting('search_path')")
            )
            schemas = [part.strip() for part in str(search_path).split(",")]
            if not schemas or schemas[0] != schema:
                raise UnsafeTestDatabaseError(
                    f"search_path={search_path!r} не начинается с {schema}: "
                    "изоляция схемы не действует, запуск прерван без изменений в БД"
                )
            # checkfirst=False: инспекция таблиц идёт по search_path и могла бы
            # найти таблицы из public; схема только что создана и пуста
            await conn.run_sync(
                metadata.create_all,
                checkfirst=False,
            )
            resolved = await conn.scalar(
                sa.text(
                    "SELECT n.nspname FROM pg_class AS c "
                    "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                    "WHERE c.oid = to_regclass('payments')"
                )
            )

        if resolved != schema:
            raise UnsafeTestDatabaseError(
                f"payments резолвится в {resolved!r}, а не в {schema}: "
                "прерываю, чтобы не писать в чужую схему"
            )

        sessionmaker = async_sessionmaker(
            bind=engine,
            expire_on_commit=False,
        )
        yield lambda: SqlAlchemyUnitOfWork(sessionmaker=sessionmaker)
    finally:
        # сначала закрываем пул: «повисшая» сессия теста держала бы блокировки
        # на таблицах схемы и мешала её удалению
        await engine.dispose()
        if created:
            await drop_own_schema(
                url,
                schema,
            )


def make_payment(key: str = "key-int-1") -> Payment:
    return Payment.create(
        payment_id=PaymentId.next(),
        amount=Money(
            amount=Decimal("55.25"),
            currency=Currency.RUB,
        ),
        description="Integration payment",
        metadata={"order_id": "7"},
        idempotency_key=IdempotencyKey(value=key),
        webhook_url=WebhookUrl(value="https://example.com/hook"),
    )


class TestPaymentRepository:
    async def test_add_and_get_roundtrip(
        self,
        uow_factory,
    ) -> None:
        uow = uow_factory()
        payment = make_payment()
        async with uow as w:
            await w.payments.add(payment=payment)
            await w.commit()

        uow2 = uow_factory()
        async with uow2 as w:
            loaded = await w.payments.get(payment_id=payment.id)
        assert loaded is not None
        assert str(loaded.id) == str(payment.id)
        assert loaded.amount.amount == Decimal("55.2500")
        assert loaded.status.value == "pending"
        assert loaded.metadata == {"order_id": "7"}

    async def test_get_by_idempotency_key(
        self,
        uow_factory,
    ) -> None:
        uow = uow_factory()
        payment = make_payment("key-int-2")
        async with uow as w:
            await w.payments.add(payment=payment)
            await w.commit()

        uow2 = uow_factory()
        async with uow2 as w:
            found = await w.payments.get_by_idempotency_key(key="key-int-2")
            missing = await w.payments.get_by_idempotency_key(key="no-such-key")
        assert found is not None
        assert found.fingerprint is None
        assert missing is None


class TestUnitOfWork:
    async def test_commit_persists_payment_and_outbox_atomically(
        self,
        uow_factory,
    ) -> None:
        uow = uow_factory()
        payment = make_payment("key-int-3")
        async with uow as w:
            await w.payments.add(payment=payment)
            await w.outbox.add(
                payload={
                    "event": "payment.created",
                    "payment_id": str(payment.id),
                },
                aggregate_id=str(payment.id),
            )
            await w.commit()

        uow2 = uow_factory()
        async with uow2 as w:
            loaded = await w.payments.get(payment_id=payment.id)
            unsent = await w.outbox.fetch_unsent(batch=10)
        assert loaded is not None
        assert len(unsent) == 1

    async def test_rollback_discards_uncommitted(
        self,
        uow_factory,
    ) -> None:
        uow = uow_factory()
        payment = make_payment("key-int-4")
        async with uow as w:
            await w.payments.add(payment=payment)
            await w.rollback()  # без commit

        uow2 = uow_factory()
        async with uow2 as w:
            loaded = await w.payments.get(payment_id=payment.id)
        assert loaded is None


class TestProcessPaymentPersistStatus:
    """Регрессия: статус должен реально обновляться в БД и не перезаписываться дважды."""

    async def test_process_persists_succeeded(
        self,
        uow_factory,
    ) -> None:
        from payment.application.ports.payment_gateway import GatewayResult
        from payment.application.services.payment_service import PaymentService

        uow = uow_factory()
        payment = make_payment("key-proc-1")
        async with uow as w:
            await w.payments.add(payment=payment)
            await w.commit()
        pid = payment.id

        class OkGateway:
            async def process(
                self,
                *,
                payment_id,
                amount,
                currency,
                idempotency_key="",
            ) -> GatewayResult:
                return GatewayResult(succeeded=True)

        service = PaymentService(
            uow=uow_factory(),
            gateway=OkGateway(),
        )
        result = await service.process_payment(payment_id=pid)

        async with uow_factory() as w:
            loaded = await w.payments.get(payment_id=pid)
        assert result.status == "succeeded"
        assert loaded is not None and loaded.status.value == "succeeded"

    async def test_claim_prevents_double_processing(
        self,
        uow_factory,
    ) -> None:
        """Клейм выдаётся один раз: второй consumer не получит токен."""
        payment = make_payment("key-proc-2")
        async with uow_factory() as w:
            await w.payments.add(payment=payment)
            await w.commit()

        async with uow_factory() as w:
            first_token = await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=60,
            )
            await w.commit()
        async with uow_factory() as w:
            second_token = await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=60,
            )
            await w.commit()

        assert first_token is not None
        assert second_token is None

    async def test_finalize_requires_matching_claim_token(
        self,
        uow_factory,
    ) -> None:
        payment = make_payment("key-proc-3")
        async with uow_factory() as w:
            await w.payments.add(payment=payment)
            await w.commit()

        async with uow_factory() as w:
            token = await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=60,
            )
            await w.commit()
        assert token is not None

        payment.mark_succeeded()
        async with uow_factory() as w:
            wrong = await w.payments.try_finalize(
                payment=payment,
                claim_token=(
                    token.replace(year=1999)
                    if hasattr(
                        token,
                        "replace",
                    )
                    else token
                ),
            )
            await w.commit()
        assert wrong is False

        async with uow_factory() as w:
            right = await w.payments.try_finalize(
                payment=payment,
                claim_token=token,
            )
            await w.commit()
        async with uow_factory() as w:
            again = await w.payments.try_finalize(
                payment=payment,
                claim_token=token,
            )
            await w.commit()

        assert right is True
        assert again is False

    async def test_release_claim_only_with_own_token(
        self,
        uow_factory,
    ) -> None:
        payment = make_payment("key-proc-5")
        async with uow_factory() as w:
            await w.payments.add(payment=payment)
            await w.commit()

        async with uow_factory() as w:
            token = await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=60,
            )
            await w.commit()

        # чужой токен не снимает клейм
        async with uow_factory() as w:
            await w.payments.release_claim(
                payment_id=payment.id,
                claim_token=token.replace(
                    year=1999,
                    microsecond=1,
                ),
            )
            await w.commit()
        async with uow_factory() as w:
            still_locked = await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=60,
            )
        assert still_locked is None

        # свой токен снимает — платёж снова доступен
        async with uow_factory() as w:
            await w.payments.release_claim(
                payment_id=payment.id,
                claim_token=token,
            )
            await w.commit()
        async with uow_factory() as w:
            reclaimed = await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=60,
            )
            await w.commit()
        assert reclaimed is not None

    async def test_expired_claim_can_be_reacquired(
        self,
        uow_factory,
    ) -> None:
        payment = make_payment("key-proc-4")
        async with uow_factory() as w:
            await w.payments.add(payment=payment)
            await w.commit()

        async with uow_factory() as w:
            await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=0.0,
            )
            await w.commit()
        async with uow_factory() as w:
            token = await w.payments.try_claim(
                payment_id=payment.id,
                ttl_seconds=60,
            )
            await w.commit()

        assert token is not None


class TestOutboxRepository:
    async def test_mark_attempt_failed_persists_even_without_publish(
        self,
        uow_factory,
    ) -> None:
        async with uow_factory() as uow:
            record = await uow.outbox.add(
                payload={"n": 1},
                aggregate_id="a1",
            )
            await uow.commit()

        async with uow_factory() as uow:
            failed = await uow.outbox.mark_attempt_failed(
                record_id=record.id,
                max_attempts=3,
            )
            await uow.commit()
        assert failed is False

        async with uow_factory() as uow:
            records = await uow.outbox.fetch_unsent(batch=10)
        assert len(records) == 1
        assert records[0].attempts == 1

    async def test_fetch_unsent_returns_only_unsent(
        self,
        uow_factory,
    ) -> None:
        async with uow_factory() as uow:
            r1 = await uow.outbox.add(
                payload={"n": 1},
                aggregate_id="a1",
            )
            await uow.outbox.add(
                payload={"n": 2},
                aggregate_id="a2",
            )
            await uow.outbox.mark_sent(record_id=r1.id)
            await uow.commit()

        async with uow_factory() as uow:
            unsent = await uow.outbox.fetch_unsent(batch=10)
        assert [r.payload["n"] for r in unsent] == [2]

    async def test_batch_limit_and_order(
        self,
        uow_factory,
    ) -> None:
        async with uow_factory() as uow:
            for i in range(5):
                await uow.outbox.add(
                    payload={"n": i},
                    aggregate_id=f"a{i}",
                )
            await uow.commit()

        async with uow_factory() as uow:
            batch = await uow.outbox.fetch_unsent(batch=3)
        assert len(batch) == 3
