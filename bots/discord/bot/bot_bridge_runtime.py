from __future__ import annotations

import logging
from typing import Any

from .config import normalize_bridge_destination_name
from .event_contracts import build_transport_event, normalize_admin_action

LOGGER = logging.getLogger("nmdiscordbot")


def _bridge_semantic_kind(self, event_kind: str) -> str:
    explicit = getattr(self.settings, 'telegram_event_semantic_aliases', {}) or {}
    if explicit:
        for semantic, aliases in explicit.items():
            if event_kind == semantic or event_kind in aliases:
                return semantic
    mapping = {
        'community.announcement.created': 'announcement_created',
        'community.event.created': 'event_created',
        'community.event.reminder': 'event_reminder',
        'community.stage.announcement': 'stage_announcement',
        'community.support.created': 'support_created',
        'community.bug_report.created': 'bug_created',
        'community.suggestion.created': 'suggestion_created',
        'community.report.created': 'report_created',
        'community.appeal.created': 'appeal_created',
        'community.guild_recruitment.created': 'guild_recruitment_created',
        'identity.discord.linked': 'identity_linked',
        'identity.discord.unlinked': 'identity_unlinked',
    }
    return mapping.get(event_kind, event_kind.replace('.', '_'))


def _bridge_destination_mapping(self) -> dict[str, str]:
    community_url = self.settings.community_core_event_url
    mapping = {
        'community_core': community_url,
        'community': community_url,
        'telegram': self.settings.telegram_bridge_url,
        'vk': self.settings.vk_bridge_url,
        'workspace': self.settings.workspace_bridge_url,
    }
    return {key: value for key, value in mapping.items() if value}


def _bridge_destinations(self) -> list[str]:
    # Preserve the historic behavior of returning URL values while avoiding
    # duplicate community/community_core aliases.
    values: list[str] = []
    for key in ('community_core', 'telegram', 'vk', 'workspace'):
        url = _bridge_destination_mapping(self).get(key)
        if url and url not in values:
            values.append(url)
    return values


def _bridge_destinations_for_event(self, event_kind: str) -> list[str]:
    rules = self.settings.bridge_event_rules
    if rules:
        allowed_targets: set[str] = set(rules.get(event_kind, ())) or set(rules.get('*', ()))
        if allowed_targets:
            mapping = _bridge_destination_mapping(self)
            selected: list[str] = []
            for raw_name in allowed_targets:
                if raw_name == '*':
                    return self._bridge_destinations()
                name = normalize_bridge_destination_name(raw_name)
                url = mapping.get(name)
                if url and url not in selected:
                    selected.append(url)
            return selected
    return self._bridge_destinations()


async def queue_bridge_event(self, event_kind: str, payload: dict[str, Any]) -> None:
    if not self._bridge_policy_allows(event_kind):
        LOGGER.info("Bridge policy dropped event %s", event_kind)
        return
    destinations = self._bridge_destinations_for_event(event_kind)
    if not destinations:
        return
    filtered_payload = self._filter_bridge_payload(event_kind, payload)
    semantic_kind = self._bridge_semantic_kind(event_kind)
    enriched_payload = dict(filtered_payload)
    enriched_payload.setdefault('semantic_kind', semantic_kind)
    enriched_payload.setdefault('event_semantics_version', 'telegram-parity-v1')
    envelope = build_transport_event(event_type=event_kind, payload=enriched_payload, source='discord-bridge', ttl_seconds=self.settings.bridge_event_ttl_seconds)
    for destination in destinations:
        await self.community_store.queue_external_sync_event(event_kind=event_kind, destination=destination, payload=envelope)


async def queue_bridge_admin_action(self, action: str, payload: dict[str, Any], *, actor_user_id: int = 0) -> None:
    destinations = self._bridge_destinations()
    if not destinations:
        return
    envelope = normalize_admin_action(action=action, payload=payload, actor_user_id=actor_user_id, ttl_seconds=self.settings.bridge_event_ttl_seconds)
    for destination in destinations:
        await self.community_store.queue_external_sync_event(event_kind=f'admin.{action}', destination=destination, payload=envelope)


def _filter_bridge_payload(self, event_kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    rules = self.settings.bridge_payload_allowlist
    allowed = tuple(rules.get(event_kind, ())) or tuple(rules.get('*', ()))
    if not allowed:
        return payload
    return {key: payload.get(key) for key in allowed if key in payload}


def _bridge_destination_label(self, destination: str) -> str:
    mapping = {
        self.settings.community_core_event_url: 'Community Core',
        self.settings.telegram_bridge_url: 'Telegram',
        self.settings.vk_bridge_url: 'VK',
        self.settings.workspace_bridge_url: 'Workspace',
    }
    return mapping.get(destination, destination or 'неизвестно')


def _bridge_policy_allows(self, event_kind: str) -> bool:
    kind = (event_kind or '').lower()
    if 'announcement' in kind:
        return self.settings.bridge_sync_announcements
    if 'event' in kind or 'stage' in kind:
        return self.settings.bridge_sync_events
    if 'support' in kind or 'suggestion' in kind or 'bug' in kind or 'appeal' in kind:
        return self.settings.bridge_sync_support
    if 'report' in kind:
        return self.settings.bridge_sync_reports
    if 'guild_recruitment' in kind:
        return self.settings.bridge_sync_guild_recruitment
    if 'identity' in kind or 'link' in kind or 'onboarding' in kind or 'interest_roles' in kind:
        return self.settings.bridge_sync_identity
    return True
