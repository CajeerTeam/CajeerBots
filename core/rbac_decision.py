from __future__ import annotations

from typing import Any


async def decide_permission(runtime: Any, event: Any, permission: str):
    """Return RBAC decision for both sync cache stores and async PostgreSQL stores.

    Modules must not call runtime.rbac_store.decide() directly because
    RBAC_BACKEND=postgres exposes only decide_async().
    """
    store = getattr(runtime, "rbac_store", None)
    if store is None:
        raise RuntimeError("runtime.rbac_store is not configured")
    decide_async = getattr(store, "decide_async", None)
    if callable(decide_async):
        return await decide_async(event, permission)
    return store.decide(event, permission)
