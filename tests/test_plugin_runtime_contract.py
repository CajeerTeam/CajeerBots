from __future__ import annotations

import asyncio
from pathlib import Path

from core.api_dispatcher import AsyncApiDispatcher
from core.config import Settings
from core.events import CajeerEvent
from core.runtime import Runtime


def _base_env(monkeypatch) -> None:
    monkeypatch.setenv("CAJEER_BOTS_ENV", "test")
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("API_TOKEN", "test-token")
    monkeypatch.setenv("API_TOKEN_READONLY", "readonly")
    monkeypatch.setenv("API_TOKEN_METRICS", "metrics")
    monkeypatch.setenv("TELEGRAM_ENABLED", "false")
    monkeypatch.setenv("DISCORD_ENABLED", "false")
    monkeypatch.setenv("VKONTAKTE_ENABLED", "false")
    monkeypatch.setenv("FAKE_ENABLED", "true")
    monkeypatch.setenv("MODULES_ENABLED", "identity,rbac,logs,bridge")


def test_plugin_api_route_is_dispatched_and_exported_in_openapi(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("PLUGINS_ENABLED", "example_api_route")

    async def scenario():
        runtime = Runtime(Settings.from_env(), Path.cwd())
        await runtime.components.start()
        try:
            dispatcher = AsyncApiDispatcher(runtime)
            status, payload, _ = await dispatcher.get("/plugins/example-api-route", actor="readonly")
            openapi_status, openapi, _ = await dispatcher.get("/openapi.json", actor="readonly")
            return status, payload, openapi_status, openapi
        finally:
            await runtime.components.stop()

    status, payload, openapi_status, openapi = asyncio.run(scenario())
    assert status == 200
    assert payload["ok"] is True
    assert payload["plugin"] == "example_api_route"
    assert openapi_status == 200
    assert "/plugins/example-api-route" in openapi["paths"]


def test_plugin_permissions_are_enforced_for_routes(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("PLUGINS_ENABLED", "example_plugin")

    async def scenario():
        runtime = Runtime(Settings.from_env(), Path.cwd())
        await runtime.components.start()
        try:
            assert runtime.plugin_routes == []
            component = next(item for item in runtime.components.loaded if item.manifest.id == "example_plugin")
            assert not component.failed
        finally:
            await runtime.components.stop()

    asyncio.run(scenario())


def test_plugin_scheduled_job_runs_in_local_scheduler(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("PLUGINS_ENABLED", "example_scheduler")

    async def scenario():
        runtime = Runtime(Settings.from_env(), Path.cwd())
        await runtime.components.start()
        try:
            assert runtime.plugin_scheduled_jobs
            ran = await runtime.scheduler.run_once()
            return ran, [event.type for event in runtime.event_bus.snapshot()]
        finally:
            await runtime.components.stop()

    ran, event_types = asyncio.run(scenario())
    assert ran >= 1
    assert "plugin.example_scheduler.tick" in event_types


def test_plugin_context_blocks_missing_permission(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("PLUGINS_ENABLED", "example_api_route")

    async def scenario():
        runtime = Runtime(Settings.from_env(), Path.cwd())
        await runtime.components.start()
        try:
            route = runtime.plugin_routes[0]
            route.context.require("api.route.register")
            try:
                route.context.require("delivery.enqueue")
            except PermissionError:
                return True
            return False
        finally:
            await runtime.components.stop()

    assert asyncio.run(scenario()) is True
