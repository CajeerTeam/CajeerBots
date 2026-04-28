from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from .config import Settings, normalize_bridge_destination_name
from .event_contracts import PAYLOAD_VALIDATORS


@dataclass(frozen=True, slots=True)
class BridgeDestinationStatus:
    name: str
    configured: bool
    url_present: bool
    routed_events: tuple[str, ...]
    warnings: tuple[str, ...]


def _destination_urls(settings: Settings) -> dict[str, str]:
    return {
        "community_core": settings.community_core_event_url,
        "telegram": settings.telegram_bridge_url,
        "vk": settings.vk_bridge_url,
        "workspace": settings.workspace_bridge_url,
    }


def _normalise_rules(settings: Settings) -> dict[str, tuple[str, ...]]:
    normalised: dict[str, tuple[str, ...]] = {}
    for event_key, destinations in (settings.bridge_event_rules or {}).items():
        normalised[event_key] = tuple(dict.fromkeys(normalize_bridge_destination_name(item) for item in destinations))
    return normalised


def build_bridge_doctor_report(settings: Settings) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    destination_urls = _destination_urls(settings)
    configured_destinations = {name for name, url in destination_urls.items() if bool(url)}
    rules = _normalise_rules(settings)

    if configured_destinations and not (settings.outbound_hmac_secret or settings.outbound_bearer_token):
        errors.append("bridge destinations are configured, but OUTBOUND_HMAC_SECRET/OUTBOUND_BEARER_TOKEN is missing")

    if settings.ingress_enabled and settings.ingress_strict_auth and not (settings.ingress_hmac_secret or settings.ingress_bearer_token):
        errors.append("INGRESS_ENABLED=true with strict auth requires INGRESS_HMAC_SECRET or INGRESS_BEARER_TOKEN")

    known_events = set(PAYLOAD_VALIDATORS) | {"*"}
    legacy_events = {"announcements", "events", "support", "reports", "guild_recruitment", "identity"}
    for event_key, destinations in rules.items():
        if event_key not in known_events and event_key not in legacy_events and not event_key.startswith(("community.", "identity.", "admin.")):
            errors.append(f"unsupported bridge event rule key: {event_key}")
        for destination in destinations:
            if destination == "*":
                continue
            if destination not in destination_urls:
                errors.append(f"unsupported bridge destination in {event_key}: {destination}")
            elif not destination_urls.get(destination):
                warnings.append(f"{event_key} routes to {destination}, but its URL is empty")

    if not rules and configured_destinations:
        warnings.append("BRIDGE_EVENT_RULES_JSON is empty; all bridge-enabled events may route to all configured destinations")

    routed_by_destination: dict[str, list[str]] = {name: [] for name in destination_urls}
    for event_key, destinations in rules.items():
        expanded = tuple(destination_urls) if "*" in destinations else destinations
        for destination in expanded:
            if destination in routed_by_destination:
                routed_by_destination[destination].append(event_key)

    statuses: list[BridgeDestinationStatus] = []
    for name, url in destination_urls.items():
        destination_warnings: list[str] = []
        if url and rules and not routed_by_destination.get(name):
            destination_warnings.append("URL is configured, but no rule routes events to this destination")
        if not url and routed_by_destination.get(name):
            destination_warnings.append("Rules route events here, but URL is empty")
        statuses.append(BridgeDestinationStatus(
            name=name,
            configured=bool(url),
            url_present=bool(url),
            routed_events=tuple(sorted(set(routed_by_destination.get(name, [])))),
            warnings=tuple(destination_warnings),
        ))
        warnings.extend(f"{name}: {item}" for item in destination_warnings)

    policy = {
        "announcements": settings.bridge_sync_announcements,
        "events": settings.bridge_sync_events,
        "support": settings.bridge_sync_support,
        "reports": settings.bridge_sync_reports,
        "guild_recruitment": settings.bridge_sync_guild_recruitment,
        "identity": settings.bridge_sync_identity,
    }

    return {
        "ok": not errors,
        "destinations": [asdict(item) for item in statuses],
        "configured_destinations": sorted(configured_destinations),
        "rule_count": len(rules),
        "rules": {key: list(value) for key, value in sorted(rules.items())},
        "outbound_auth": {
            "hmac_configured": bool(settings.outbound_hmac_secret),
            "bearer_configured": bool(settings.outbound_bearer_token),
            "key_id": settings.outbound_key_id,
        },
        "ingress": {
            "enabled": settings.ingress_enabled,
            "strict_auth": settings.ingress_strict_auth,
            "hmac_configured": bool(settings.ingress_hmac_secret),
            "bearer_configured": bool(settings.ingress_bearer_token),
            "bind": f"{settings.ingress_host}:{settings.ingress_port}",
        },
        "policy": policy,
        "known_event_contract_keys": sorted(PAYLOAD_VALIDATORS),
        "warnings": warnings,
        "errors": errors,
    }


def build_bridge_route_preview(settings: Settings, event_kind: str) -> dict[str, Any]:
    """Build a focused route preview for one bridge event type."""
    event_key = str(event_kind or "").strip()
    destination_urls = _destination_urls(settings)
    rules = _normalise_rules(settings)
    raw_targets = tuple(rules.get(event_key, ())) or tuple(rules.get("*", ()))
    used_fallback = event_key not in rules and "*" in rules
    if not raw_targets:
        resolved_targets = tuple(name for name, url in destination_urls.items() if url)
        routing_mode = "default_all_configured_destinations"
    elif "*" in raw_targets:
        resolved_targets = tuple(name for name, url in destination_urls.items() if url)
        routing_mode = "wildcard"
    else:
        resolved_targets = tuple(dict.fromkeys(destination for destination in raw_targets if destination in destination_urls and destination_urls.get(destination)))
        routing_mode = "explicit"

    missing_urls = [
        destination
        for destination in raw_targets
        if destination != "*" and destination in destination_urls and not destination_urls.get(destination)
    ]
    unsupported_targets = [
        destination
        for destination in raw_targets
        if destination != "*" and destination not in destination_urls
    ]

    return {
        "ok": not unsupported_targets,
        "event_kind": event_key,
        "routing_mode": routing_mode,
        "used_wildcard_fallback": used_fallback,
        "payload_validator_exists": event_key in PAYLOAD_VALIDATORS,
        "known_event_contract": event_key in PAYLOAD_VALIDATORS or event_key.startswith(("community.", "identity.", "admin.")),
        "resolved_destinations": list(resolved_targets),
        "configured_destinations": sorted(name for name, url in destination_urls.items() if url),
        "destination_urls": {name: bool(url) for name, url in destination_urls.items()},
        "missing_destination_urls": missing_urls,
        "unsupported_targets": unsupported_targets,
        "outbound_auth": {
            "hmac_configured": bool(settings.outbound_hmac_secret),
            "bearer_configured": bool(settings.outbound_bearer_token),
            "key_id": settings.outbound_key_id,
        },
        "policy": {
            "announcements": settings.bridge_sync_announcements,
            "events": settings.bridge_sync_events,
            "support": settings.bridge_sync_support,
            "reports": settings.bridge_sync_reports,
            "guild_recruitment": settings.bridge_sync_guild_recruitment,
            "identity": settings.bridge_sync_identity,
        },
    }


def run_bridge_doctor(settings: Settings) -> int:
    report = build_bridge_doctor_report(settings)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 4
