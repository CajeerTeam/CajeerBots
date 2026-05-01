from __future__ import annotations

from typing import Any


class RuntimeDbResources:
    """Общий lifecycle-holder для SQLAlchemy async engine.

    Постепенно новые repository/service-классы должны брать engine отсюда,
    чтобы не создавать разрозненные connection pools на каждый backend.
    """

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self._async_engine: Any | None = None

    def async_engine(self) -> Any:
        if self._async_engine is None:
            if not self.settings.storage.async_database_url:
                raise RuntimeError("DATABASE_ASYNC_URL не задан")
            from sqlalchemy.ext.asyncio import create_async_engine

            self._async_engine = create_async_engine(self.settings.storage.async_database_url, pool_pre_ping=True)
        return self._async_engine

    async def close(self) -> None:
        if self._async_engine is not None:
            await self._async_engine.dispose()
            self._async_engine = None
