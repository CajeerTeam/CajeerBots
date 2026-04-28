from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from typing import Any

import aiohttp

from .config import Settings
from .event_contracts import normalize_admin_action
from .services.bridge_client import push_external_event


@dataclass(frozen=True, slots=True)
class IngressSmokeAttempt:
    name: str
    endpoint: str
    expected_status: str
    ok: bool
    latency_ms: int | None
    error: str


def _public_endpoint(settings: Settings, path: str = "/internal/bridge/admin") -> str | None:
    base = getattr(settings, "app_public_url", "") or ""
    if not base:
        return None
    return f"{base.rstrip('/')}{path}"


def _endpoint(settings: Settings, path: str = "/internal/bridge/admin") -> str:
    host = settings.ingress_host
    if host in {"0.0.0.0", "::", ""}:
        host = "127.0.0.1"
    return f"http://{host}:{settings.ingress_port}{path}"


def _build_admin_smoke(settings: Settings) -> dict[str, Any]:
    return normalize_admin_action(
        action="smoke_test",
        actor_user_id=0,
        ttl_seconds=settings.bridge_event_ttl_seconds,
        payload={
            "kind": "smoke_test",
            "title": "NeverMine ingress smoke test",
            "source": "nmdiscordbot-cli",
            "smoke_test": True,
        },
    )


async def build_ingress_smoke_report(settings: Settings) -> dict[str, Any]:
    warnings: list[str] = []
    errors: list[str] = []
    attempts: list[IngressSmokeAttempt] = []

    if not settings.ingress_enabled:
        warnings.append("INGRESS_ENABLED=false; HTTP smoke can only pass while ingress is enabled and the bot process is running")
    if settings.ingress_strict_auth and not (settings.ingress_hmac_secret or settings.ingress_bearer_token):
        errors.append("strict ingress auth is enabled but INGRESS_HMAC_SECRET/INGRESS_BEARER_TOKEN is missing")

    endpoint = _endpoint(settings)
    public_endpoint = _public_endpoint(settings)
    if public_endpoint:
        warnings.append(f"public HTTP server endpoint configured: {public_endpoint}")
    payload = _build_admin_smoke(settings)

    async with aiohttp.ClientSession() as session:
        started = time.perf_counter()
        try:
            ok = await push_external_event(
                session,
                endpoint,
                payload,
                bearer_token=settings.ingress_bearer_token,
                hmac_secret=settings.ingress_hmac_secret,
                key_id="default",
                timeout_seconds=settings.bridge_timeout_seconds,
            )
            attempts.append(IngressSmokeAttempt(
                name="valid_signature",
                endpoint=endpoint,
                expected_status="2xx",
                ok=bool(ok),
                latency_ms=int((time.perf_counter() - started) * 1000),
                error="" if ok else "push_failed",
            ))
            if not ok:
                errors.append("valid_signature: push_failed")
        except Exception as exc:  # noqa: BLE001
            attempts.append(IngressSmokeAttempt(
                name="valid_signature",
                endpoint=endpoint,
                expected_status="2xx",
                ok=False,
                latency_ms=int((time.perf_counter() - started) * 1000),
                error=str(exc),
            ))
            errors.append(f"valid_signature: {exc}")

        if settings.ingress_strict_auth and settings.ingress_hmac_secret:
            started = time.perf_counter()
            try:
                await push_external_event(
                    session,
                    endpoint,
                    payload,
                    bearer_token=settings.ingress_bearer_token,
                    hmac_secret="wrong-" + settings.ingress_hmac_secret,
                    key_id="default",
                    timeout_seconds=settings.bridge_timeout_seconds,
                )
                attempts.append(IngressSmokeAttempt(
                    name="bad_signature_rejection",
                    endpoint=endpoint,
                    expected_status="401",
                    ok=False,
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    error="bad signature was accepted",
                ))
                errors.append("bad_signature_rejection: bad signature was accepted")
            except aiohttp.ClientResponseError as exc:
                ok = exc.status == 401
                attempts.append(IngressSmokeAttempt(
                    name="bad_signature_rejection",
                    endpoint=endpoint,
                    expected_status="401",
                    ok=ok,
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    error="" if ok else f"unexpected_status:{exc.status}",
                ))
                if not ok:
                    errors.append(f"bad_signature_rejection: unexpected status {exc.status}")
            except Exception as exc:  # noqa: BLE001
                attempts.append(IngressSmokeAttempt(
                    name="bad_signature_rejection",
                    endpoint=endpoint,
                    expected_status="401",
                    ok=False,
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    error=str(exc),
                ))
                errors.append(f"bad_signature_rejection: {exc}")

    return {
        "ok": not errors,
        "endpoint": endpoint,
        "ingress_enabled": settings.ingress_enabled,
        "strict_auth": settings.ingress_strict_auth,
        "attempts": [asdict(item) for item in attempts],
        "warnings": warnings,
        "errors": errors,
    }


def run_ingress_smoke(settings: Settings) -> int:
    report = asyncio.run(build_ingress_smoke_report(settings))
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 4
