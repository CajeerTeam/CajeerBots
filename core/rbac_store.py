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


    async def decide_async(self, event: Any, permission: str) -> RbacDecision:
        return self.decide(event, permission)


class PostgresRbacStore:
    """DB-backed RBAC reader matching the current Alembic schema."""

    def __init__(self, async_dsn: str, schema: str, *, fallback_to_event_grants: bool = True, engine: Any | None = None) -> None:
        self.async_dsn = async_dsn
        self.schema = schema
        self.fallback_to_event_grants = fallback_to_event_grants
        self._engine: Any | None = engine
        self._owns_engine = engine is None

    def _event_key(self, event: Any) -> tuple[str, str] | None:
        actor = getattr(event, "actor", None)
        if actor is None:
            return None
        platform = getattr(actor, "platform", "")
        platform_user_id = getattr(actor, "platform_user_id", "")
        return (platform, platform_user_id) if platform and platform_user_id else None

    async def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine

            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def close(self) -> None:
        if self._engine is not None and self._owns_engine:
            await self._engine.dispose()
        self._engine = None

    async def grants_for_event_async(self, event: Any) -> tuple[set[str], str]:
        from sqlalchemy import text
        from core.permissions import grants_from_event
        from core.schema import validate_schema_name

        direct = grants_from_event(event)
        if direct and self.fallback_to_event_grants:
            return direct, "event"
        key = self._event_key(event)
        if key is None:
            return set(), "postgres:none"
        schema = validate_schema_name(self.schema)
        engine = await self._engine_obj()
        async with engine.begin() as conn:
            rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT rp.permission
                          FROM {schema}.platform_accounts pa
                          JOIN {schema}.user_roles ur ON ur.user_id = pa.user_id
                          JOIN {schema}.role_permissions rp ON rp.role_id = ur.role_id
                         WHERE pa.platform = :platform
                           AND pa.platform_user_id = :platform_user_id
                        """
                    ),
                    {"platform": key[0], "platform_user_id": key[1]},
                )
            ).fetchall()
        grants = {str(row[0]) for row in rows if row and row[0]}
        return grants, "postgres" if grants else "postgres:none"

    async def decide_async(self, event: Any, permission: str) -> RbacDecision:
        grants, source = await self.grants_for_event_async(event)
        return RbacDecision("*" in grants or permission in grants, grants, source)

    def decide(self, event: Any, permission: str) -> RbacDecision:
        raise RuntimeError("PostgresRbacStore требует async decide_async()")


class CascadingRbacStore:
    """RBAC chain: event grants -> PostgreSQL -> local cache."""

    def __init__(self, postgres: PostgresRbacStore, cache: HybridRbacStore) -> None:
        self.postgres = postgres
        self.cache = cache

    def reload(self) -> None:
        self.cache.reload()

    def snapshot(self) -> dict[str, Any]:
        return {"backend": "hybrid", "cache": self.cache.snapshot()}

    async def close(self) -> None:
        await self.postgres.close()

    async def decide_async(self, event: Any, permission: str) -> RbacDecision:
        from core.permissions import grants_from_event

        direct = grants_from_event(event)
        if direct:
            return RbacDecision("*" in direct or permission in direct, direct, "event")
        pg = await self.postgres.decide_async(event, permission)
        if pg.grants:
            return pg
        return self.cache.decide(event, permission)

    def decide(self, event: Any, permission: str) -> RbacDecision:
        return self.cache.decide(event, permission)


def build_rbac_store(settings: Any, db_resources: Any | None = None) -> Any:
    cache = HybridRbacStore(settings.runtime_dir / "secrets" / "rbac_cache.json")
    backend = getattr(settings, "rbac_backend", "cache")
    if backend == "cache":
        return cache
    postgres = PostgresRbacStore(settings.storage.async_database_url, settings.shared_schema, engine=(db_resources.async_engine() if db_resources is not None else None))
    if backend == "postgres":
        return postgres
    return CascadingRbacStore(postgres, cache)



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
    """Bootstrap the first owner directly in PostgreSQL-backed RBAC tables.

    The SQL must track the Alembic/ORM contract:
    - users.user_id, not users.id
    - platform_accounts has a composite primary key (platform, platform_user_id)
    - roles.role_id/title/source, not roles.id/name/description
    - role_permissions stores permission text directly
    - audit_log.audit_id, not audit_log.id
    """
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
    role_id = f"role_{role}"
    engine = create_async_engine(async_dsn, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.users(user_id, display_name, created_at, updated_at)
                VALUES (:user_id, :display_name, NOW(), NOW())
                ON CONFLICT (user_id) DO UPDATE
                SET display_name = EXCLUDED.display_name, updated_at = NOW()
            """), {"user_id": user_id, "display_name": display_name or platform_user_id})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.platform_accounts(platform, platform_user_id, user_id, username, display_name, profile, created_at, updated_at)
                VALUES (:platform, :platform_user_id, :user_id, NULL, :display_name, '{{}}'::jsonb, NOW(), NOW())
                ON CONFLICT (platform, platform_user_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    display_name = EXCLUDED.display_name,
                    updated_at = NOW()
            """), {"user_id": user_id, "platform": platform, "platform_user_id": platform_user_id, "display_name": display_name or platform_user_id})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.roles(role_id, title, source, created_at)
                VALUES (:role_id, :title, 'local-bootstrap', NOW())
                ON CONFLICT (role_id) DO UPDATE
                SET title = EXCLUDED.title, source = EXCLUDED.source
            """), {"role_id": role_id, "title": role})
            for permission in grants:
                await conn.execute(text(f"""
                    INSERT INTO {schema_name}.role_permissions(role_id, permission)
                    VALUES (:role_id, :permission)
                    ON CONFLICT (role_id, permission) DO NOTHING
                """), {"role_id": role_id, "permission": permission})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.user_roles(user_id, role_id, granted_at)
                VALUES (:user_id, :role_id, NOW())
                ON CONFLICT (user_id, role_id) DO NOTHING
            """), {"user_id": user_id, "role_id": role_id})
            await conn.execute(text(f"""
                INSERT INTO {schema_name}.audit_log(audit_id, actor_type, actor_id, action, resource, result, trace_id, message, created_at)
                VALUES (:audit_id, 'system', 'cli', 'rbac.bootstrap_owner', :resource, 'ok', :trace_id, :message, NOW())
            """), {"audit_id": "aud_" + uuid4().hex, "resource": f"{platform}:{platform_user_id}", "trace_id": "cli", "message": f"role={role};permissions={','.join(grants)}"})
    finally:
        await engine.dispose()
    return {
        "ok": True,
        "backend": "postgres",
        "schema": schema_name,
        "account": f"{platform}:{platform_user_id}",
        "user_id": user_id,
        "role": role,
        "permissions": sorted(set(grants)),
    }

