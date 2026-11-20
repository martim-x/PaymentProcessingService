"""Тесты предохранителей integration-набора: тесты не имеют права трогать рабочую БД.

Проверяем чистые функции из `tests.integration.test_persistence`, поэтому файл
не требует PostgreSQL: ни одного реального подключения здесь не происходит.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from tests.integration import test_persistence as isolation

REPO_ROOT = Path(__file__).resolve().parents[2]


class TestResolveTestDatabaseUrl:
    def test_missing_test_url_is_refused(self) -> None:
        with pytest.raises(isolation.UnsafeTestDatabaseError) as exc:
            isolation.resolve_test_database_url({})
        assert isolation.TEST_DATABASE_URL_ENV in str(exc.value)

    def test_blank_test_url_is_refused(self) -> None:
        with pytest.raises(isolation.UnsafeTestDatabaseError):
            isolation.resolve_test_database_url({isolation.TEST_DATABASE_URL_ENV: "   "})

    def test_registry_database_url_is_not_used_as_fallback(self) -> None:
        """PAYMENT_DATABASE_URL рабочего стека не подхватывается автоматически."""
        environ = {"PAYMENT_DATABASE_URL": "postgresql+psycopg://payment:x@db:5432/payments"}
        with pytest.raises(isolation.UnsafeTestDatabaseError) as exc:
            isolation.resolve_test_database_url(environ)
        assert "PAYMENT_DATABASE_URL" in str(exc.value)

    @pytest.mark.parametrize(
        "url",
        [
            "sqlite+aiosqlite:///./test.db",
            "postgresql+asyncpg://payment:x@localhost:5432/payments",
            "mysql+pymysql://payment:x@localhost/payments",
        ],
    )
    def test_driver_without_libpq_options_is_refused(self, url: str) -> None:
        """Изоляция строится на libpq `options=-csearch_path=...`, нужен psycopg."""
        with pytest.raises(isolation.UnsafeTestDatabaseError):
            isolation.resolve_test_database_url({isolation.TEST_DATABASE_URL_ENV: url})

    def test_psycopg_dsn_is_accepted(self) -> None:
        url = "postgresql+psycopg://payment:x@localhost:5432/payments_test"
        assert isolation.resolve_test_database_url({isolation.TEST_DATABASE_URL_ENV: url}) == url


class TestOwnSchemaOnly:
    def test_schema_name_is_unique_and_prefixed(self) -> None:
        names = {isolation.new_schema_name() for _ in range(500)}
        assert len(names) == 500
        pattern = re.compile(r"^pytest_[0-9a-f]{12}$")
        assert all(pattern.match(name) for name in names)

    def test_drop_targets_only_generated_schema_name(self) -> None:
        schema = isolation.new_schema_name()
        assert isolation.drop_schema_sql(schema) == f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'

    @pytest.mark.parametrize(
        "schema",
        [
            "public",
            "payments",
            "outbox",
            'pytest_0123456789ab"; DROP TABLE payments; --',
            "pytest_0123456789AB",
            "pytest_short",
            "Pytest_0123456789ab",
            "",
        ],
    )
    def test_drop_is_refused_for_foreign_names(self, schema: str) -> None:
        with pytest.raises(isolation.UnsafeTestDatabaseError):
            isolation.drop_schema_sql(schema)

    async def test_drop_validates_name_before_connecting(self) -> None:
        """Отказ по имени происходит до подключения: сервер даже не нужен."""
        with pytest.raises(isolation.UnsafeTestDatabaseError):
            await isolation.drop_own_schema(
                "postgresql+psycopg://payment:x@127.0.0.1:5999/unreachable", "public"
            )


class TestIntegrationSuiteCannotRunImplicitly:
    def test_suite_skips_when_test_database_url_is_unset(self) -> None:
        """Без явного PAYMENT_TEST_DATABASE_URL набор не должен подключаться к БД вовсе."""
        env = {k: v for k, v in os.environ.items() if k != isolation.TEST_DATABASE_URL_ENV}
        env["PAYMENT_DATABASE_URL"] = "postgresql+psycopg://payment:x@localhost:5432/payments"
        env.pop("PYTEST_ADDOPTS", None)

        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/integration", "-q", "-p", "no:cacheprovider"],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode == 0, output
        assert "skipped" in output, output
        assert "passed" not in output.replace("skipped", ""), output
