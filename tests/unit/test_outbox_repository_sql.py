"""SQL outbox: relay обязан видеть только записи dispatch='relay'.

Семантика `dispatch='local'` защищена именно SQL-фильтром в `fetch_unsent`:
если условие пропадёт, webhook-команда уедет в `payments.new` вторым путём
доставки. Тест компилирует statement postgres-диалектом, без живой БД.
"""

from sqlalchemy.dialects import postgresql

from payment.adapters.persistence.repositories.outbox import SqlAlchemyOutboxRepository
from payment.application.ports.repositories import DISPATCH_LOCAL, DISPATCH_RELAY


class FakeResult:
    def first(self) -> None:
        return None

    def __iter__(self) -> object:
        return iter(())


class CapturingSession:
    def __init__(self) -> None:
        self.statements: list = []

    async def execute(self, statement):
        self.statements.append(statement)
        return FakeResult()


def compile_sql(statement) -> str:
    return " ".join(str(statement.compile(dialect=postgresql.dialect())).split())


def bind_params(statement) -> dict:
    return statement.compile(dialect=postgresql.dialect()).params


class TestFetchUnsentSql:
    async def test_query_filters_by_relay_dispatch(self) -> None:
        session = CapturingSession()
        repo = SqlAlchemyOutboxRepository(session)  # type: ignore[arg-type]

        rows = await repo.fetch_unsent(batch=10)

        assert rows == []
        sql = compile_sql(session.statements[-1])
        assert "dispatch" in sql, sql
        params = bind_params(session.statements[-1])
        assert DISPATCH_RELAY in params.values()
        assert "outbox.sent IS false" in sql
        assert "outbox.failed IS false" in sql
        assert "FOR UPDATE SKIP LOCKED" in sql

    async def test_local_dispatch_is_not_in_the_selection(self) -> None:
        session = CapturingSession()
        repo = SqlAlchemyOutboxRepository(session)  # type: ignore[arg-type]

        await repo.fetch_unsent(batch=5)

        params = bind_params(session.statements[-1])
        assert DISPATCH_LOCAL not in params.values()


class TestAddSql:
    async def test_insert_keeps_record_unpublished(self) -> None:
        session = CapturingSession()
        repo = SqlAlchemyOutboxRepository(session)  # type: ignore[arg-type]

        record = await repo.add(
            payload={"event": "payment.webhook", "event_id": "evt-1"},
            aggregate_id="p1",
            dispatch=DISPATCH_LOCAL,
        )

        params = bind_params(session.statements[-1])
        assert record.dispatch == DISPATCH_LOCAL
        assert record.sent is False
        assert DISPATCH_LOCAL in params.values()
        assert False in params.values()  # sent=False в самом INSERT
