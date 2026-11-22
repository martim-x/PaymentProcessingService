"""Контракты владения кодом и строковых значений в БД."""

import ast
from pathlib import Path


class TestRuntimeOwnership:
    def test_all_runtime_functions_have_class_owners(self) -> None:
        runtime = Path(__file__).resolve().parents[3] / "src" / "payment"
        modules = sorted(runtime.rglob("*.py"))
        assert modules
        violations = []
        for path in modules:
            tree = ast.parse(path.read_bytes(), filename=str(path))
            method_ids = {
                id(member)
                for owner in ast.walk(tree)
                if isinstance(owner, ast.ClassDef)
                for member in owner.body
            }
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if id(node) not in method_ids:
                        violations.append(
                            f"{path.relative_to(runtime)}:{node.lineno}: {node.name}"
                        )
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    targets = node.targets
                elif isinstance(node, ast.AnnAssign):
                    targets = [node.target]
                else:
                    continue
                for target in targets:
                    if isinstance(target, ast.Name) and target.id.isupper():
                        violations.append(
                            f"{path.relative_to(runtime)}:{node.lineno}: {target.id}"
                        )
        assert not violations, "runtime ownership violations:\n" + "\n".join(violations)


def test_enum_database_literals_remain_stable() -> None:
    from payment.application.ports.repositories import (
        OutboxDispatch,
        WebhookDeliveryStatus,
    )

    assert [member.value for member in WebhookDeliveryStatus] == [
        "pending",
        "delivering",
        "delivered",
        "failed",
    ]
    assert [member.value for member in OutboxDispatch] == ["relay", "local"]


def test_enum_model_defaults_keep_external_literals() -> None:
    from payment.adapters.persistence.orm import outbox_table, webhook_deliveries_table
    from payment.application.ports.repositories import (
        OutboxDispatch,
        WebhookDeliveryStatus,
    )

    assert outbox_table.c.dispatch.default.arg == OutboxDispatch.RELAY.value
    assert (
        webhook_deliveries_table.c.status.default.arg
        == WebhookDeliveryStatus.PENDING.value
    )
    index = next(
        index
        for index in outbox_table.indexes
        if index.name == "ix_outbox_relay_pending"
    )
    assert "dispatch = 'relay'" in str(index.dialect_options["postgresql"]["where"])
