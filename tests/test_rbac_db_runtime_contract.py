from __future__ import annotations

import inspect

from core.rbac_store import PostgresRbacStore, build_rbac_store


def test_postgres_rbac_store_is_async_reader():
    assert inspect.iscoroutinefunction(PostgresRbacStore.decide_async)


def test_rbac_builder_respects_backend(monkeypatch):
    monkeypatch.setenv("RBAC_BACKEND", "cache")
    from core.config import Settings
    settings = Settings.from_env()
    store = build_rbac_store(settings)
    assert store.__class__.__name__ == "HybridRbacStore"
