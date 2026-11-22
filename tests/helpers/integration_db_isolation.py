"""Общие предохранители интеграционных тестов БД и их unit-контрактов."""

import re
import uuid
from collections.abc import Mapping

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

TEST_DATABASE_URL_ENV = "PAYMENT_TEST_DATABASE_URL"
APP_DATABASE_URL_ENV = "PAYMENT_DATABASE_URL"
SCHEMA_PREFIX = "pytest_"

# Изоляция опирается на libpq `options=-csearch_path=...`, т.е. на psycopg3.
_REQUIRED_DSN_PREFIX = "postgresql+psycopg://"
_OWN_SCHEMA_RE = re.compile(r"pytest_[0-9a-f]{12}")


class UnsafeTestDatabaseError(RuntimeError):
    """Конфигурация integration-тестов небезопасна — запуск отклоняется."""


def resolve_test_database_url(environ: Mapping[str, str]) -> str:
    """Возвращает DSN тестовой БД или отказывается запускаться.

    Рабочий DSN не используется как значение по умолчанию ни при каких условиях.
    """
    url = (environ.get(TEST_DATABASE_URL_ENV) or "").strip()
    if not url:
        app_url_set = bool((environ.get(APP_DATABASE_URL_ENV) or "").strip())
        fallback = (
            f"; fallback на {APP_DATABASE_URL_ENV} запрещён"
            if app_url_set
            else "; значение по умолчанию для рабочей БД запрещено"
        )
        raise UnsafeTestDatabaseError(
            f"{TEST_DATABASE_URL_ENV} не задан — integration-тесты требуют явную тестовую БД"
            f"{fallback}"
        )
    if not url.startswith(_REQUIRED_DSN_PREFIX):
        raise UnsafeTestDatabaseError(
            f"{TEST_DATABASE_URL_ENV} должен быть DSN psycopg3 ({_REQUIRED_DSN_PREFIX}...): "
            "изоляция схемы опирается на libpq options=-csearch_path"
        )
    return url


def new_schema_name() -> str:
    """Уникальное имя собственной схемы теста."""
    return f"{SCHEMA_PREFIX}{uuid.uuid4().hex[:12]}"


def drop_schema_sql(schema: str) -> str:
    """SQL удаления ТОЛЬКО собственной схемы; иначе — отказ."""
    if not _OWN_SCHEMA_RE.fullmatch(schema):
        raise UnsafeTestDatabaseError(
            f"отказ удалять схему {schema!r}: удаляются только схемы вида {SCHEMA_PREFIX}<12 hex>"
        )
    return f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'


async def drop_own_schema(
    url: str,
    schema: str,
) -> None:
    """Удаляет схему теста на отдельном соединении с обычным search_path."""
    sql = drop_schema_sql(schema)  # имя проверено до подключения
    engine = create_async_engine(
        url,
        poolclass=NullPool,
    )
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql("SET LOCAL lock_timeout = '5s'")
            await conn.exec_driver_sql("SET LOCAL statement_timeout = '10s'")
            await conn.exec_driver_sql(sql)
    finally:
        await engine.dispose()
