from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlparse

import aiohttp

from .config import Settings, normalize_bridge_destination_name
from .event_contracts import build_transport_event
from .services.bridge_client import push_external_event


@dataclass(frozen=True, slots=True)
class BridgeSmokeResult:
    destination: str
    url_configured: bool
    endpoint: str
    routed: bool
    ok: bool
    latency_ms: int | None
    error: str


def _redact_url(url: str) -> str:
    parsed = urlparse(url or "")
    if not parsed.scheme or not parsed.netloc:
        return "<empty>"
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path or '/'}"


def _destination_urls(settings: Settings) -> dict[str, str]:
    return {
        "community_core": settings.community_core_event_url,
        "telegram": settings.telegram_bridge_url,
        "vk": settings.vk_bridge_url,
        "workspace": settings.workspace_bridge_url,
    }


def _routed_destination_names(settings: Settings, event_type: str) -> list[str]:
    urls = _destination_urls(settings)
    rules = settings.bridge_event_rules or {}
    raw_targets = tuple(rules.get(event_type, ())) or tuple(rules.get("*", ()))
    if not raw_targets:
        return [name for name, url in urls.items() if url]
    if "*" in raw_targets:
        return [name for name, url in urls.items() if url]
    result: list[str] = []
    for raw_target in raw_targets:
        target = normalize_bridge_destination_name(raw_target)
        if target in urls and urls[target] and target not in result:
            result.append(target)
    return result


def _build_smoke_event(settings: Settings, event_type: str) -> dict[str, Any]:
    return build_transport_event(
        event_type=event_type,
        source="discord-bridge-smoke",
        ttl_seconds=settings.bridge_event_ttl_seconds,
        payload={
            "title": "NeverMine bridge smoke test",
            "text": "Synthetic bridge smoke event from NMDiscordBot. This confirms outbound HMAC delivery.",
            "body": "Synthetic bridge smoke event from NMDiscordBot. This confirms outbound HMAC delivery.",
            "url": settings.nevermine_website_url or settings.nevermine_discord_invite_url or "",
            "source_platform": "discord",
            "smoke_test": True,
        },
    )


async def build_bridge_smoke_report(settings: Settings, *, event_type: str = "community.world_signal.created") -> dict[str, Any]:
    destination_urls = _destination_urls(settings)
    routed_names = _routed_destination_names(settings, event_type)
    event = _build_smoke_event(settings, event_type)
    results: list[BridgeSmokeResult] = []
    errors: list[str] = []
    warnings: list[str] = []

    if not routed_names:
        warnings.append(f"no configured/routed destinations for {event_type}")

    if not (settings.outbound_hmac_secret or settings.outbound_bearer_token):
        warnings.append("OUTBOUND_HMAC_SECRET/OUTBOUND_BEARER_TOKEN is missing; smoke will be unsigned unless destination accepts unsigned traffic")

    async with aiohttp.ClientSession() as session:
        for name in routed_names:
            url = destination_urls.get(name, "")
            started = time.perf_counter()
            try:
                ok = await push_external_event(
                    session,
                    url,
                    event,
                    bearer_token=settings.outbound_bearer_token,
                    hmac_secret=settings.outbound_hmac_secret,
                    key_id=settings.outbound_key_id,
                    timeout_seconds=settings.bridge_timeout_seconds,
                )
                latency = int((time.perf_counter() - started) * 1000)
                results.append(BridgeSmokeResult(
                    destination=name,
                    url_configured=bool(url),
                    endpoint=_redact_url(url),
                    routed=True,
                    ok=bool(ok),
                    latency_ms=latency,
                    error="" if ok else "push_failed",
                ))
                if not ok:
                    errors.append(f"{name}: push_failed")
            except Exception as exc:  # noqa: BLE001 - diagnostic command must report exact runtime failure
                latency = int((time.perf_counter() - started) * 1000)
                results.append(BridgeSmokeResult(
                    destination=name,
                    url_configured=bool(url),
                    endpoint=_redact_url(url),
                    routed=True,
                    ok=False,
                    latency_ms=latency,
                    error=str(exc),
                ))
                errors.append(f"{name}: {exc}")

    return {
        "ok": not errors,
        "event_type": event_type,
        "event_id": event.get("event_id"),
        "routed_destinations": routed_names,
        "results": [asdict(item) for item in results],
        "warnings": warnings,
        "errors": errors,
    }


def run_bridge_smoke(settings: Settings, *, event_type: str = "community.world_signal.created") -> int:
    report = asyncio.run(build_bridge_smoke_report(settings, event_type=event_type))
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 4
