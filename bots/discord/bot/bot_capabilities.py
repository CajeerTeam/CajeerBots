from __future__ import annotations

from typing import Any

from .bot_routing import required_subscription_event_kinds
from .bot_snapshot import snapshot_missing_sections


def build_capability_report(
    *,
    runtime_version: str,
    build_info: dict[str, Any],
    grouped_command_count: int,
    schema_version: int,
    schema_parity_issues: list[str],
    migration_count: int,
    extra_checks: dict[str, Any] | None = None,
    flat_command_count: int = 0,
    restore_capabilities: list[str] | None = None,
) -> dict[str, Any]:
    features = {str(item) for item in build_info.get('features') or []}
    restore_capabilities = sorted({str(item) for item in (restore_capabilities or []) if str(item)})
    routing_required = required_subscription_event_kinds()
    extra_checks = dict(extra_checks or {})
    contract_coverage = extra_checks.pop('contract_coverage', {}) if isinstance(extra_checks.get('contract_coverage'), dict) else {}
    snapshot_sections = extra_checks.pop('snapshot_sections', {}) if isinstance(extra_checks.get('snapshot_sections'), dict) else {}
    checks: dict[str, Any] = {
        'slash_groups_declared': 'command-groups' in features,
        'approval_queue_declared': 'approval-queue' in features,
        'targeted_digests_declared': 'staff-digest' in features or 'recurring-targeted-digests' in features,
        'runtime_version_matches_build': str(build_info.get('version') or '').strip() == runtime_version,
        'schema_parity_ok': not schema_parity_issues,
        'grouped_surface_primary': int(grouped_command_count or 0) > 0,
        'restore_capability_count': len(restore_capabilities),
        'routing_required_event_count': len(routing_required),
        'routing_required_events': routing_required,
        'contract_coverage': contract_coverage,
        'snapshot_sections': snapshot_sections,
    }
    checks.update(extra_checks)
    declared_runtime = {
        'command_groups': 'command-groups' in features,
        'approval_expiry': 'approval-expiry-sweeper' in features or 'approval-expiry' in features,
        'comment_lifecycle': 'comment-edit-delete-parity' in features,
        'state_restore_symmetry': 'state-restore-symmetry' in features or 'state-restore' in features,
        'subscription_routing': 'subscription-live-routing' in features or 'subscription-routing-wildcards' in features,
        'recurring_digests': 'recurring-targeted-digests' in features,
        'calendar_scheduler': 'calendar-digest-scheduler' in features,
        'transport_mirror_registry': 'external-discussion-mirror-registry' in features and 'external-content-mirror-registry' in features,
        'legacy_lifecycle': 'legacy-layout-resource-lifecycle' in features,
        'transport_contract_runtime': 'transport-contract-runtime-coverage' in features,
        'schema_migration_model': 'community-schema-migration-model-v2' in features,
    }
    readiness = {
        'runtime_version': runtime_version,
        'build_version': str(build_info.get('version') or '').strip(),
        'schema_version': int(schema_version or 0),
        'migration_count': int(migration_count or 0),
        'grouped_command_count': int(grouped_command_count or 0),
        'flat_command_count': int(flat_command_count or 0),
        'schema_parity_issues': schema_parity_issues,
        'restore_capabilities': restore_capabilities,
        'checks': checks,
        'declared_runtime': declared_runtime,
    }
    coverage_missing = bool(contract_coverage.get('declared_only') or contract_coverage.get('handled_only') or contract_coverage.get('routing_without_validator') or contract_coverage.get('routing_without_handler'))
    snapshot_missing = bool(snapshot_sections.get('missing_required_sections') or snapshot_missing_sections(snapshot_sections.get('raw_snapshot') if isinstance(snapshot_sections, dict) else {}))
    readiness['ready'] = bool(checks.get('runtime_version_matches_build')) and not schema_parity_issues and int(grouped_command_count or 0) > 0 and not coverage_missing and not snapshot_missing
    return readiness
