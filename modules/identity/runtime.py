from __future__ import annotations

from hashlib import sha256

from core.events import CajeerEvent


class IdentityModule:
    id = "identity"

    async def on_start(self, context) -> None:
        context.logger.info("модуль identity запущен")

    def _identity_id(self, event: CajeerEvent) -> str | None:
        if event.actor is None:
            return None
        raw = f"{event.actor.platform}:{event.actor.platform_user_id}"
        return "usr_" + sha256(raw.encode("utf-8")).hexdigest()[:24]

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        identity_id = self._identity_id(event)
        if not identity_id:
            return None
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action="identity.resolve", resource=identity_id, trace_id=event.trace_id)
        try:
            if context.runtime.settings.storage.async_database_url and event.actor is not None:
                from core.repositories.business import BusinessStateRepository
                profile = {"platform": event.actor.platform, "platform_user_id": event.actor.platform_user_id, "display_name": event.actor.display_name}
                await BusinessStateRepository(context.runtime.settings.storage.async_database_url, context.runtime.settings.shared_schema).upsert_identity(
                    user_id=identity_id,
                    platform=event.actor.platform,
                    platform_user_id=event.actor.platform_user_id,
                    display_name=event.actor.display_name or "",
                    profile=profile,
                )
        except Exception:
            pass
        return {"identity_id": identity_id, "platform": event.source, "trace_id": event.trace_id}

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "identity":
            return None
        identity_id = self._identity_id(event)
        return {"ok": True, "message": f"Пользователь идентифицирован: {identity_id}", "identity_id": identity_id, "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль identity остановлен")
