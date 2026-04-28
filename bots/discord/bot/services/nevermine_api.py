from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

import aiohttp


class NeverMineApiError(RuntimeError):
    pass


@dataclass(slots=True)
class NeverMineApiClient:
    base_url: str
    api_token: str
    status_endpoint: str
    players_endpoint: str
    announcements_endpoint: str
    events_endpoint: str
    verify_start_endpoint: str
    verify_complete_endpoint: str
    link_status_endpoint: str
    link_unlink_endpoint: str
    timeout: float = 8.0
    retries: int = 3
    retry_backoff_seconds: float = 1.0
    retry_backoff_max_seconds: float = 8.0
    session: aiohttp.ClientSession | None = field(default=None, init=False)
    stale_cache_seconds: int = 30
    circuit_open_seconds: int = 60
    _cache: dict[str, tuple[float, dict[str, Any]]] = field(default_factory=dict, init=False)
    _circuit_open_until: float = field(default=0.0, init=False)

    def configured(self) -> bool:
        return bool(self.base_url)

    def verification_configured(self) -> bool:
        return bool(self.base_url and self.verify_start_endpoint and self.verify_complete_endpoint)

    async def open(self) -> None:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(limit=20, enable_cleanup_closed=True)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
        self.session = None

    def _headers(self, request_id: str) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "X-Request-Id": request_id,
        }
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def _url(self, endpoint: str, params: dict[str, Any] | None = None) -> str:
        if not self.base_url:
            raise NeverMineApiError("API base URL is not configured")
        url = f"{self.base_url}{endpoint}"
        if params:
            filtered = {k: v for k, v in params.items() if v is not None}
            if filtered:
                url += ("&" if "?" in url else "?") + urlencode(filtered)
        return url

    def _cache_key(self, method: str, endpoint: str, params: dict[str, Any] | None = None) -> str:
        raw = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        return f"{method.upper()}:{endpoint}?{raw}"

    def _cache_get(self, key: str) -> dict[str, Any] | None:
        cached = self._cache.get(key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at < time.time():
            self._cache.pop(key, None)
            return None
        return dict(payload)

    def _cache_put(self, key: str, payload: dict[str, Any]) -> None:
        self._cache[key] = (time.time() + max(5, int(self.stale_cache_seconds or 30)), dict(payload))

    async def _request_json(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.session is None or self.session.closed:
            await self.open()
        assert self.session is not None

        url = self._url(endpoint, params=params)
        cache_key = self._cache_key(method, endpoint, params)
        if self._circuit_open_until > time.time():
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached
            raise NeverMineApiError(f'Circuit open for {url}')
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request_id = str(uuid.uuid4())
            try:
                async with self.session.request(method, url, json=json_body, headers=self._headers(request_id)) as response:
                    if 500 <= response.status < 600:
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                            message="server error",
                            headers=response.headers,
                        )
                    response.raise_for_status()
                    data = await response.json(content_type=None)
                    if isinstance(data, dict):
                        self._cache_put(cache_key, data)
                        self._circuit_open_until = 0.0
                        return data
                    if isinstance(data, list):
                        payload = {"items": data}
                        self._cache_put(cache_key, payload)
                        self._circuit_open_until = 0.0
                        return payload
                    raise NeverMineApiError(f"Unexpected response type from {url}")
            except asyncio.TimeoutError as exc:
                last_error = exc
            except aiohttp.ClientResponseError as exc:
                last_error = exc
                if exc.status == 429:
                    retry_after = 0.0
                    if exc.headers:
                        try:
                            retry_after = float(exc.headers.get('Retry-After') or 0.0)
                        except Exception:
                            retry_after = 0.0
                    if retry_after > 0:
                        await asyncio.sleep(min(retry_after, self.retry_backoff_max_seconds))
                if 400 <= exc.status < 500 and exc.status not in {408, 429}:
                    raise NeverMineApiError(f"HTTP {exc.status} for {url}") from exc
            except aiohttp.ClientError as exc:
                last_error = exc

            if attempt >= self.retries:
                break
            await asyncio.sleep(min(self.retry_backoff_seconds * (2 ** (attempt - 1)), self.retry_backoff_max_seconds))

        cached = self._cache_get(cache_key)
        if cached is not None:
            self._circuit_open_until = max(self._circuit_open_until, time.time() + max(5, int(self.circuit_open_seconds or 60)))
            return cached
        self._circuit_open_until = max(self._circuit_open_until, time.time() + max(5, int(self.circuit_open_seconds or 60)))
        if isinstance(last_error, asyncio.TimeoutError):
            raise NeverMineApiError(f"Request timeout: {url}") from last_error
        if isinstance(last_error, aiohttp.ClientResponseError):
            raise NeverMineApiError(f"HTTP {last_error.status} for {url}") from last_error
        if isinstance(last_error, aiohttp.ClientError):
            raise NeverMineApiError(f"HTTP error for {url}: {last_error}") from last_error
        raise NeverMineApiError(f"Request failed for {url}")

    async def fetch_status(self) -> dict[str, Any]:
        return await self._request_json("GET", self.status_endpoint)

    async def fetch_players(self) -> dict[str, Any]:
        return await self._request_json("GET", self.players_endpoint)

    async def fetch_announcements(self) -> list[dict[str, Any]]:
        data = await self._request_json("GET", self.announcements_endpoint)
        items = data.get("items") or data.get("announcements") or []
        return [item for item in items if isinstance(item, dict)]

    async def fetch_events(self) -> list[dict[str, Any]]:
        data = await self._request_json("GET", self.events_endpoint)
        items = data.get("items") or data.get("events") or []
        return [item for item in items if isinstance(item, dict)]

    async def start_verification(self, discord_user_id: int, discord_username: str) -> dict[str, Any]:
        return await self._request_json(
            "POST",
            self.verify_start_endpoint,
            json_body={"discord_user_id": str(discord_user_id), "discord_username": discord_username},
        )

    async def complete_verification(self, discord_user_id: int, code: str) -> dict[str, Any]:
        return await self._request_json(
            "POST",
            self.verify_complete_endpoint,
            json_body={"discord_user_id": str(discord_user_id), "code": code},
        )

    async def fetch_link_status(self, discord_user_id: int) -> dict[str, Any]:
        return await self._request_json("GET", self.link_status_endpoint, params={"discord_user_id": str(discord_user_id)})

    async def unlink(self, discord_user_id: int) -> dict[str, Any]:
        return await self._request_json("POST", self.link_unlink_endpoint, json_body={"discord_user_id": str(discord_user_id)})
