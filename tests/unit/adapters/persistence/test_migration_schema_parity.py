"""Схема из миграций и ORM-метаданные не должны расходиться.

Тест не требует БД: `upgrade()` прогоняется на подменённом `alembic.op`,
который только записывает операции, а результат сравнивается с `metadata`
из `payment.adapters.persistence.orm`. Так расходятся ли индексы/уникальные
ограничения видно ещё до запуска PostgreSQL и до `alembic upgrade head`.

Источник истины — миграции: ORM обязан описывать то же самое, иначе
`metadata.create_all` (используется в integration-тестах) создаёт схему
без индексов, и тесты проверяют не ту схему, что в проде.
"""

import importlib.util
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from payment.adapters.persistence.orm import metadata

pytestmark = [
    pytest.mark.unit,
    pytest.mark.adapters,
]


VERSIONS_DIR = Path(__file__).resolve().parents[4] / "migrations" / "versions"


class RecordedSchema:
    """Схема, восстановленная из вызовов `alembic.op`."""

    def __init__(self) -> None:
        # отдельный MetaData: `sa.Table` достраивает неявные ограничения
        # (`Column(unique=True)`, `_pending_colargs`) — иначе колонки уникальных
        # ограничений из миграции читаются пустыми
        self._metadata = sa.MetaData()
        self.tables: dict[str, dict[str, sa.Column]] = {}
        self.uniques: dict[str, set[tuple[str, ...]]] = {}
        self.indexes: dict[str, tuple[str, tuple[str, ...], str | None]] = {}

    def add_table(
        self,
        name: str,
        items: tuple,
        kwargs: dict,
    ) -> None:
        if kwargs.get("schema") is not None:  # pragma: no cover - защита от смены API
            raise AssertionError(
                f"create_table(schema=...) не поддержан тестом: {name}"
            )
        table = sa.Table(
            name,
            self._metadata,
            *items,
        )
        self.tables[name] = {column.name: column for column in table.columns}
        self.uniques[name] = {
            tuple(sorted(constraint.columns.keys()))
            for constraint in table.constraints
            if isinstance(
                constraint,
                sa.UniqueConstraint,
            )
        }

    def add_index(
        self,
        name: str,
        table: str,
        columns: tuple[str, ...],
        where: str | None,
    ) -> None:
        self.indexes[name] = (
            table,
            columns,
            where,
        )


class _FakeOp:
    """Минимальный двойник `alembic.op` для офлайн-прогона миграций."""

    def __init__(
        self,
        *,
        schema: RecordedSchema,
    ) -> None:
        self._schema = schema

    def create_table(
        self,
        name: str,
        *items,
        **kwargs,
    ) -> None:
        self._schema.add_table(
            name,
            items,
            kwargs,
        )

    def create_index(
        self,
        name: str,
        table: str,
        columns: list[str],
        **kwargs,
    ) -> None:
        where = kwargs.get("postgresql_where")
        self._schema.add_index(
            name,
            table,
            tuple(columns),
            _normalize_expression(where),
        )

    def drop_index(
        self,
        name: str,
        table_name: str | None = None,
        **kwargs,
    ) -> None:
        self._schema.indexes.pop(
            name,
            None,
        )

    def drop_table(
        self,
        name: str,
        **kwargs,
    ) -> None:
        self._schema.tables.pop(
            name,
            None,
        )
        self._schema.uniques.pop(
            name,
            None,
        )

    def add_column(
        self,
        table: str,
        column: sa.Column,
        **kwargs,
    ) -> None:
        self._schema.tables[table][column.name] = column

    def drop_column(
        self,
        table: str,
        column_name: str,
        **kwargs,
    ) -> None:
        self._schema.tables[table].pop(
            column_name,
            None,
        )

    def __getattr__(
        self,
        item: str,
    ):
        raise AssertionError(
            f"тест не умеет op.{item}: расширьте _FakeOp, иначе паритет схемы не проверен"
        )


def _normalize_expression(expression) -> str | None:
    if expression is None:
        return None
    text = " ".join(str(expression).lower().split())
    return text or None


def _normalize_default(column: sa.Column) -> str | None:
    """server_default колонки в нормализованном виде.

    Сравнивать нужно именно server_default: integration-тесты строят схему из
    `metadata`, поэтому расхождение с миграцией означает, что тесты проверяют
    не ту схему, что в проде (так уже было с outbox.dispatch).
    """
    default = column.server_default
    if default is None:
        return None
    argument = getattr(
        default,
        "arg",
        default,
    )
    text = (
        str(argument)
        if not isinstance(
            argument,
            str,
        )
        else argument
    )
    normalized = " ".join(text.lower().split())
    return normalized or None


def _normalize_type(column_type: sa.types.TypeEngine) -> str:
    return str(column_type.compile(dialect=postgresql.dialect())).upper()


def _load_module(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        f"_migration_{path.stem}",
        path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _migrations_in_order() -> Iterator[ModuleType]:
    """Все ревизии в порядке применения: от корня (down_revision is None) к голове."""
    modules = {
        module.revision: module
        for module in (_load_module(path) for path in sorted(VERSIONS_DIR.glob("*.py")))
    }
    children: dict[str, list[str]] = {}
    roots: list[str] = []
    for revision, module in modules.items():
        if module.down_revision is None:
            roots.append(revision)
        else:
            children.setdefault(
                module.down_revision,
                [],
            ).append(revision)
    if len(roots) != 1:
        raise AssertionError(f"ожидалась одна корневая миграция, найдено: {roots}")

    ordered: list[ModuleType] = []
    seen: set[str] = set()

    def visit(revision: str) -> None:
        if revision in seen:
            raise AssertionError(f"цикл или дубль в цепочке миграций на {revision}")
        seen.add(revision)
        ordered.append(modules[revision])
        for child in sorted(
            children.get(
                revision,
                [],
            )
        ):
            visit(child)

    visit(roots[0])
    if len(ordered) != len(modules):
        raise AssertionError(f"не связаны с корнем: {sorted(set(modules) - seen)}")
    return iter(ordered)


@pytest.fixture(scope="module")
def migrated_schema() -> RecordedSchema:
    schema = RecordedSchema()
    fake_op = _FakeOp(schema=schema)
    for module in _migrations_in_order():
        vars(module)["op"] = fake_op  # from alembic import op — подмена для теста
        module.upgrade()
    return schema


@pytest.fixture(scope="module")
def orm_schema() -> RecordedSchema:
    schema = RecordedSchema()
    for table in metadata.tables.values():
        schema.tables[table.name] = {column.name: column for column in table.columns}
        schema.uniques[table.name] = {
            tuple(sorted(constraint.columns.keys()))
            for constraint in table.constraints
            if isinstance(
                constraint,
                sa.UniqueConstraint,
            )
        }
        for index in table.indexes:
            where = index.dialect_options["postgresql"].get("where")
            schema.add_index(
                index.name or "",
                table.name,
                tuple(column.name for column in index.columns),
                _normalize_expression(where),
            )
    return schema


class TestTableParity:
    def test_same_tables(
        self,
        migrated_schema,
        orm_schema,
    ) -> None:
        assert set(orm_schema.tables) == set(migrated_schema.tables)

    def test_same_columns(
        self,
        migrated_schema,
        orm_schema,
    ) -> None:
        for name in sorted(migrated_schema.tables):
            migrated = migrated_schema.tables[name]
            orm = orm_schema.tables.get(
                name,
                {},
            )
            assert set(orm) == set(migrated), f"{name}: набор колонок расходится"

            for column_name in sorted(migrated):
                expected = migrated[column_name]
                actual = orm[column_name]
                assert _normalize_type(actual.type) == _normalize_type(expected.type), (
                    f"{name}.{column_name}: тип {_normalize_type(actual.type)} "
                    f"в ORM, {_normalize_type(expected.type)} в миграции"
                )
                assert bool(actual.nullable) == bool(expected.nullable), (
                    f"{name}.{column_name}: nullable={actual.nullable} в ORM, "
                    f"{expected.nullable} в миграции"
                )
                assert _normalize_default(actual) == _normalize_default(expected), (
                    f"{name}.{column_name}: DEFAULT {_normalize_default(actual)!r} в ORM, "
                    f"{_normalize_default(expected)!r} в миграции"
                )


class TestUniqueConstraintParity:
    def test_same_unique_constraints(
        self,
        migrated_schema,
        orm_schema,
    ) -> None:
        for name in sorted(migrated_schema.tables):
            assert (
                orm_schema.uniques.get(
                    name,
                    set(),
                )
                == migrated_schema.uniques[name]
            ), f"{name}: уникальные ограничения расходятся"


class TestIndexParity:
    def test_indexes_are_declared_in_orm_at_all(
        self,
        orm_schema,
    ) -> None:
        """Защита от вырожденного сравнения: индексы в ORM вообще должны быть."""
        assert orm_schema.indexes, "в ORM не объявлено ни одного индекса"

    def test_same_index_names(
        self,
        migrated_schema,
        orm_schema,
    ) -> None:
        assert set(orm_schema.indexes) == set(migrated_schema.indexes), (
            "набор индексов расходится: "
            f"в ORM {sorted(orm_schema.indexes)}, в миграции {sorted(migrated_schema.indexes)}"
        )

    def test_same_index_definitions(
        self,
        migrated_schema,
        orm_schema,
    ) -> None:
        for name in sorted(set(migrated_schema.indexes) & set(orm_schema.indexes)):
            assert orm_schema.indexes[name] == migrated_schema.indexes[name], (
                f"{name}: (таблица, колонки, predicate) в ORM {orm_schema.indexes[name]}, "
                f"в миграции {migrated_schema.indexes[name]}"
            )
