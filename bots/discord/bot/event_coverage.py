from __future__ import annotations

import json
from typing import Any

from .config import Settings, normalize_bridge_destination_name
from .event_contracts import PAYLOAD_VALIDATORS, declared_transport_event_types
from .bot_transport import handled_transport_event_types


def _destination_urls(settings: Settings) -> dict[str, str]:
    return {
        "community_core": settings.community_core_event_url,
        "telegram": settings.telegram_bridge_url,
        "vk": settings.vk_bridge_url,
        "workspace": settings.workspace_bridge_url,
    }


def _routes_for_event(settings: Settings, event_type: str) -> list[str]:
    rules = settings.bridge_event_rules or {}
    urls = _destination_urls(settings)
    targets = tuple(rules.get(event_type, ())) or tuple(rules.get("*", ()))
    if not targets:
        return [name for name, url in urls.items() if url]
    if "*" in targets:
        return [name for name, url in urls.items() if url]
    result: list[str] = []
    for target in targets:
        name = normalize_bridge_destination_name(target)
        if name in urls and urls[name] and name not in result:
            result.append(name)
    return result


def build_event_coverage_report(settings: Settings) -> dict[str, Any]:
    declared = set(declared_transport_event_types())
    validators = set(PAYLOAD_VALIDATORS)
    inbound = set(handled_transport_event_types())
    routed: dict[str, list[str]] = {}
    unrouted: list[str] = []
    unknown_rule_keys: list[str] = []
    rules = settings.bridge_event_rules or {}

    for event_type in sorted(declared):
        routes = _routes_for_event(settings, event_type)
        if routes:
            routed[event_type] = routes
        else:
            unrouted.append(event_type)

    for key in rules:
        if key != "*" and key not in declared and not key.startswith(("admin.", "identity.", "community.")):
            unknown_rule_keys.append(key)

    destination_counts: dict[str, int] = {name: 0 for name in _destination_urls(settings)}
    for routes in routed.values():
        for route in routes:
            destination_counts[route] = destination_counts.get(route, 0) + 1

    bridge_policy = {
        "announcements": settings.bridge_sync_announcements,
        "events": settings.bridge_sync_events,
        "support": settings.bridge_sync_support,
        "reports": settings.bridge_sync_reports,
        "guild_recruitment": settings.bridge_sync_guild_recruitment,
        "identity": settings.bridge_sync_identity,
    }

    return {
        "ok": not unknown_rule_keys,
        "totals": {
            "declared_event_types": len(declared),
            "payload_validators": len(validators),
            "inbound_handlers": len(inbound),
            "routed_event_types": len(routed),
            "unrouted_event_types": len(unrouted),
            "rule_count": len(rules),
        },
        "destination_counts": destination_counts,
        "configured_destinations": sorted(name for name, url in _destination_urls(settings).items() if url),
        "routed_events": routed,
        "unrouted_events": unrouted,
        "inbound_only_events": sorted(inbound - declared),
        "declared_but_not_inbound": sorted(declared - inbound),
        "unknown_rule_keys": sorted(unknown_rule_keys),
        "bridge_policy": bridge_policy,
    }


def render_event_coverage_summary(report: dict[str, Any], *, max_items: int = 8) -> str:
    totals = report.get("totals") or {}
    lines = [
        f"Declared: {totals.get('declared_event_types', 0)}",
        f"Routed: {totals.get('routed_event_types', 0)}",
        f"Unrouted: {totals.get('unrouted_event_types', 0)}",
        f"Rules: {totals.get('rule_count', 0)}",
    ]
    destination_counts = report.get("destination_counts") or {}
    if destination_counts:
        lines.append("Destinations: " + ", ".join(f"{k}={v}" for k, v in sorted(destination_counts.items())))
    unrouted = list(report.get("unrouted_events") or [])
    if unrouted:
        lines.append("Unrouted sample: " + ", ".join(unrouted[:max_items]))
    return "\n".join(lines)


def run_event_coverage(settings: Settings) -> int:
    report = build_event_coverage_report(settings)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 4
