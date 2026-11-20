"""SQL-level проверка fencing: оба UPDATE обязаны фильтровать по токену клейма.

Тест не требует живой БД: перехватывает statement и компилирует его
postgres-диалектом, проверяя предикаты и параметры.
"""

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.dialects import postgresql

from payment.adapters.persistence.repositories.webhook import (
    SqlAlchemyWebhookDeliveryRepository,
)


class FakeResult:
    def __init__(self, row: object | None = None) -> None:
        self._row = row

    def first(self) -> object | None:
        return self._row


class CapturingSession:
    def __init__(self, row: object | None = None) -> None:
        self.statements: list = []
        self._row = row

    async def execute(self, statement):
        self.statements.append(statement)
        return FakeResult(self._row)


def compile_sql(statement) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))


def param_values(statement) -> dict:
    return statement.compile(dialect=postgresql.dialect()).params


class Row:
    def __init__(self, **kwargs) -> None:
        self.__dict__.update(kwargs)


TOKEN = datetime.now(timezone.utc) + timedelta(seconds=30)
EVENT_ID = uuid.uuid4().hex


class TestMarkDeliveredSql:
    async def test_update_is_fenced_by_claim_token(self) -> None:
        session = CapturingSession(row=Row(event_id=EVENT_ID))
        repo = SqlAlchemyWebhookDeliveryRepository(session)  # type: ignore[arg-type]

        recorded = await repo.mark_delivered(EVENT_ID, TOKEN)

        assert recorded is True
        sql = compile_sql(session.statements[-1])
        assert "UPDATE webhook_deliveries" in sql
        assert "webhook_deliveries.event_id = " in sql
        assert "webhook_deliveries.status = " in sql
        assert "webhook_deliveries.locked_until = " in sql
        assert "RETURNING webhook_deliveries.event_id" in sql
        assert set(param_values(session.statements[-1]).values()) >= {EVENT_ID, TOKEN}

    async def test_no_row_means_stale_owner(self) -> None:
        session = CapturingSession(row=None)
        repo = SqlAlchemyWebhookDeliveryRepository(session)  # type: ignore[arg-type]

        assert await repo.mark_delivered(EVENT_ID, TOKEN) is False


class TestMarkFailedSql:
    async def test_update_is_fenced_by_claim_token(self) -> None:
        session = CapturingSession(row=Row(attempts=1, status="pending"))
        repo = SqlAlchemyWebhookDeliveryRepository(session)  # type: ignore[arg-type]

        outcome = await repo.mark_attempt_failed(EVENT_ID, TOKEN, 3, "boom")

        assert outcome == (1, False)
        sql = compile_sql(session.statements[-1])
        assert "webhook_deliveries.status = " in sql
        assert "webhook_deliveries.locked_until = " in sql
        assert "CASE WHEN" in sql  # exhausted -> failed, иначе pending
        assert set(param_values(session.statements[-1]).values()) >= {EVENT_ID, TOKEN}

    async def test_no_row_means_claim_lost(self) -> None:
        session = CapturingSession(row=None)
        repo = SqlAlchemyWebhookDeliveryRepository(session)  # type: ignore[arg-type]

        assert await repo.mark_attempt_failed(EVENT_ID, TOKEN, 3, "boom") is None


class TestReleaseClaimSql:
    async def test_release_is_fenced_by_claim_token(self) -> None:
        session = CapturingSession()
        repo = SqlAlchemyWebhookDeliveryRepository(session)  # type: ignore[arg-type]

        await repo.release_claim(EVENT_ID, TOKEN)

        sql = compile_sql(session.statements[-1])
        assert "webhook_deliveries.locked_until = " in sql
        assert "webhook_deliveries.status = " in sql
        assert set(param_values(session.statements[-1]).values()) >= {EVENT_ID, TOKEN}
