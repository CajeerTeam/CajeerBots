from __future__ import annotations

from typing import Any


class RuntimeDbResources:
    """Общий lifecycle-holder для SQLAlchemy async engine/sessionmaker.

    Backend-и получают engine отсюда, чтобы не создавать отдельный pool
    на каждый сервис: delivery, audit, RBAC, scheduler и будущие repositories.
    """

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self._async_engine: Any | None = None
        self._async_sessionmaker: Any | None = None

    def async_engine(self) -> Any:
        if self._async_engine is None:
            if not self.settings.storage.async_database_url:
                raise RuntimeError("DATABASE_ASYNC_URL не задан")
            from sqlalchemy.ext.asyncio import create_async_engine

            self._async_engine = create_async_engine(self.settings.storage.async_database_url, pool_pre_ping=True)
        return self._async_engine

    def async_sessionmaker(self) -> Any:
        if self._async_sessionmaker is None:
            from sqlalchemy.ext.asyncio import async_sessionmaker

            self._async_sessionmaker = async_sessionmaker(self.async_engine(), expire_on_commit=False)
        return self._async_sessionmaker

    def repository_kwargs(self) -> dict[str, Any]:
        return {
            "async_dsn": self.settings.storage.async_database_url,
            "schema": self.settings.shared_schema,
            "engine": self.async_engine() if self.settings.storage.async_database_url else None,
        }

    async def close(self) -> None:
        if self._async_engine is not None:
            await self._async_engine.dispose()
            self._async_engine = None
            self._async_sessionmaker = None
