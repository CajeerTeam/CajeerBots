from __future__ import annotations

from typing import Any

_RESTORE_SECTIONS = [
    'maintenance_mode',
    'runtime_forum_policy_overrides',
    'panel_registry',
    'layout_alias_bindings',
    'topics',
    'scheduled_jobs',
    'failed_bridge_events',
    'bridge_destination_state',
    'schema_meta',
    'schema_meta_ledger',
    'panel_drift',
    'runtime_markers',
    'bridge_comment_mirror',
    'external_discussion_mirror',
    'external_content_mirror',
    'content_pack_meta',
    'layout_spec_meta',
    'runtime_markers_snapshot',
    'build_metadata',
    'diagnostics',
]


def restore_capability_sections() -> list[str]:
    return list(_RESTORE_SECTIONS)


def snapshot_required_sections() -> list[str]:
    return [
        'topics',
        'scheduled_jobs',
        'failed_bridge_events',
        'bridge_destination_state',
        'bridge_comment_mirror',
        'external_discussion_mirror',
        'external_content_mirror',
    ]


def snapshot_missing_sections(snapshot: dict[str, Any] | None) -> list[str]:
    payload = snapshot if isinstance(snapshot, dict) else {}
    missing: list[str] = []
    for key in snapshot_required_sections():
        if payload.get(key) is None:
            missing.append(key)
    return missing


def snapshot_restore_coverage(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    payload = snapshot if isinstance(snapshot, dict) else {}
    present = sorted(key for key in _RESTORE_SECTIONS if payload.get(key) is not None)
    missing = snapshot_missing_sections(payload)
    return {
        'present_sections': present,
        'present_count': len(present),
        'required_sections': snapshot_required_sections(),
        'missing_required_sections': missing,
        'mirror_snapshot_complete': not missing,
    }
