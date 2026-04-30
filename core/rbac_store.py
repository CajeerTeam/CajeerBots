from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class RbacDecision:
    allowed: bool
    grants: set[str] = field(default_factory=set)
    source: str = "event"

    def to_dict(self) -> dict[str, object]:
        return {"allowed": self.allowed, "grants": sorted(self.grants), "source": self.source}


class HybridRbacStore:
    """Hybrid RBAC: event grants → local cache synced from Workspace → deny."""

    def __init__(self, cache_path: Path) -> None:
        self.cache_path = cache_path
        self._cache: dict[str, Any] = {}
        self.reload()

    def reload(self) -> None:
        if not self.cache_path.exists():
            self._cache = {}
            return
        try:
            self._cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            self._cache = {}


    def snapshot(self) -> dict[str, Any]:
        return self._cache if isinstance(self._cache, dict) else {}

    def save(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(
            json.dumps(self.snapshot(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def bootstrap_owner(
        self,
        *,
        platform: str,
        platform_user_id: str,
        display_name: str | None = None,
        role: str = "owner",
        permissions: list[str] | set[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        platform = platform.strip()
        platform_user_id = platform_user_id.strip()
        role = (role or "owner").strip()
        if not platform or not platform_user_id:
            raise ValueError("platform и user_id обязательны")
        grants = [str(item).strip() for item in (permissions or ["*"]) if str(item).strip()] or ["*"]
        cache = self.snapshot()
        users = cache.setdefault("users", {})
        roles = cache.setdefault("roles", {})
        roles[role] = sorted(set(grants))
        key = f"{platform}:{platform_user_id}"
        current = users.get(key, {}) if isinstance(users.get(key, {}), dict) else {}
        users[key] = {
            "roles": sorted(set([role, *current.get("roles", [])])),
            "permissions": sorted(set(current.get("permissions", []))),
            "display_name": display_name or current.get("display_name") or "",
        }
        cache["version"] = 1
        cache["source"] = "local-bootstrap"
        self._cache = cache
        self.save()
        return {
            "ok": True,
            "path": str(self.cache_path),
            "account": key,
            "role": role,
            "permissions": sorted(set(grants)),
        }

    def _event_key(self, event: Any) -> str | None:
        actor = getattr(event, "actor", None)
        if actor is None:
            return None
        if getattr(actor, "identity_id", None):
            return f"identity:{actor.identity_id}"
        platform = getattr(actor, "platform", "")
        platform_user_id = getattr(actor, "platform_user_id", "")
        return f"{platform}:{platform_user_id}" if platform and platform_user_id else None

    def grants_for_event(self, event: Any) -> tuple[set[str], str]:
        from core.permissions import grants_from_event

        direct = grants_from_event(event)
        if direct:
            return direct, "event"
        key = self._event_key(event)
        users = self._cache.get("users", {}) if isinstance(self._cache, dict) else {}
        roles = self._cache.get("roles", {}) if isinstance(self._cache, dict) else {}
        if key and key in users:
            grants: set[str] = set(users[key].get("permissions", []))
            for role in users[key].get("roles", []):
                grants.update(roles.get(role, []))
            return grants, "workspace-cache"
        return set(), "none"

    def decide(self, event: Any, permission: str) -> RbacDecision:
        grants, source = self.grants_for_event(event)
        allowed = "*" in grants or permission in grants
        return RbacDecision(allowed, grants, source)
