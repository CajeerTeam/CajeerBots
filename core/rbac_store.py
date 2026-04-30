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



async def bootstrap_owner_db(
    *,
    async_dsn: str,
    schema: str,
    platform: str,
    platform_user_id: str,
    display_name: str | None = None,
    role: str = "owner",
    permissions: list[str] | set[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Bootstrap the first owner directly in PostgreSQL-backed RBAC tables."""
    import hashlib
    from uuid import uuid4

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    from core.schema import validate_schema_name

    schema_name = validate_schema_name(schema)
    platform = platform.strip()
    platform_user_id = platform_user_id.strip()
    role = (role or "owner").strip()
    if not platform or not platform_user_id:
        raise ValueError("platform и user_id обязательны")
    grants = [str(item).strip() for item in (permissions or ["*"]) if str(item).strip()] or ["*"]
    digest = hashlib.sha256(f"{platform}:{platform_user_id}".encode("utf-8")).hexdigest()[:24]
    user_id = f"usr_{digest}"
    account_id = f"acc_{digest}"
    role_id = f"role_{role}"
    engine = create_async_engine(async_dsn, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.users(id, display_name, created_at, updated_at)
                VALUES (:user_id, :display_name, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET display_name = EXCLUDED.display_name, updated_at = NOW()
            """), {"user_id": user_id, "display_name": display_name or platform_user_id})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.platform_accounts(id, user_id, platform, platform_user_id, created_at, updated_at)
                VALUES (:account_id, :user_id, :platform, :platform_user_id, NOW(), NOW())
                ON CONFLICT (platform, platform_user_id) DO UPDATE SET user_id = EXCLUDED.user_id, updated_at = NOW()
            """), {"account_id": account_id, "user_id": user_id, "platform": platform, "platform_user_id": platform_user_id})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.roles(id, name, description, created_at, updated_at)
                VALUES (:role_id, :role, :description, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, updated_at = NOW()
            """), {"role_id": role_id, "role": role, "description": "Bootstrap owner role"})
            for permission in grants:
                permission_id = f"perm_{hashlib.sha256(permission.encode('utf-8')).hexdigest()[:24]}"
                await conn.execute(text(f"""
                    INSERT INTO {schema_name}.permissions(id, name, description, created_at, updated_at)
                    VALUES (:permission_id, :permission, :description, NOW(), NOW())
                    ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description, updated_at = NOW()
                """), {"permission_id": permission_id, "permission": permission, "description": "Bootstrap permission"})
                await conn.execute(text(f"""
                    INSERT INTO {schema_name}.role_permissions(role_id, permission_id, created_at)
                    SELECT :role_id, id, NOW() FROM {schema_name}.permissions WHERE name = :permission
                    ON CONFLICT (role_id, permission_id) DO NOTHING
                """), {"role_id": role_id, "permission": permission})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.user_roles(user_id, role_id, created_at)
                VALUES (:user_id, :role_id, NOW())
                ON CONFLICT (user_id, role_id) DO NOTHING
            """), {"user_id": user_id, "role_id": role_id})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.audit_log(id, actor_type, actor_id, action, resource, result, trace_id, message, created_at)
                VALUES (:id, 'system', 'cli', 'rbac.bootstrap_owner', :resource, 'ok', :trace_id, :message, NOW())
            """), {"id": "aud_" + uuid4().hex, "resource": f"{platform}:{platform_user_id}", "trace_id": "cli", "message": f"role={role};permissions={','.join(grants)}"})
    finally:
        await engine.dispose()
    return {"ok": True, "backend": "postgres", "schema": schema_name, "account": f"{platform}:{platform_user_id}", "user_id": user_id, "role": role, "permissions": sorted(set(grants))}
