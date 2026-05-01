import pytest

from core.rbac_store import RbacDecision
from modules.rbac.runtime import RbacModule
from modules.support.runtime import SupportModule


class AsyncOnlyStore:
    def decide(self, event, permission):  # pragma: no cover
        raise RuntimeError("sync decide must not be used")

    async def decide_async(self, event, permission):
        return RbacDecision(True, {permission}, "test")


class Runtime:
    rbac_store = AsyncOnlyStore()

    class Audit:
        def write(self, **kwargs):
            return None

    audit = Audit()


class Context:
    runtime = Runtime()

    class Logger:
        def info(self, *args, **kwargs): pass
        def warning(self, *args, **kwargs): pass

    logger = Logger()


class Event:
    trace_id = "trace"
    payload = {"args": "bots.support.reply"}

    class Actor:
        platform_user_id = "u1"

    class Chat:
        platform_chat_id = "c1"

    actor = Actor()
    chat = Chat()


@pytest.mark.asyncio
async def test_rbac_module_uses_decide_async():
    result = await RbacModule().on_command("rbac", Event(), Context())
    assert result["ok"] is True


@pytest.mark.asyncio
async def test_support_module_uses_decide_async_for_reply(monkeypatch):
    async def noop(*args, **kwargs):
        return None

    runtime = Runtime()
    runtime.workspace = type("Workspace", (), {"report_event": noop})()
    runtime.make_system_event = lambda *args, **kwargs: object()
    runtime.settings = type("Settings", (), {"storage": type("Storage", (), {"async_database_url": ""})(), "shared_schema": "shared", "support_strict_persistence": False})()
    ctx = Context()
    ctx.runtime = runtime
    event = Event()
    event.payload = {"args": "reply T-1 ok"}
    result = await SupportModule().on_command("support", event, ctx)
    assert result["ok"] is True
