from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
import csv
import hashlib
import io
import json
import logging
import mimetypes
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from . import __version__
from .community_store import COMMUNITY_SCHEMA_MIGRATIONS, CommunityStore
from .discord_panels import HelpPanelView, InterestRolesView, OnboardingView, build_help_embed, build_interest_roles_embed, build_onboarding_embed, content_schema_version, ensure_content_layout, get_faq_text, get_help_text, get_ops_text, get_panel_content, get_panel_version, get_topic_template, validate_content_pack
from .config import Settings
from .event_contracts import build_signed_response, build_transport_event, normalize_admin_action, declared_transport_event_types
from .http_ingress import BridgeIngressServer
from .locale import audit_category_label, audit_status_label, bool_label, normalize_approval_decision, normalize_approval_status, normalize_help_topic, normalize_support_area, normalize_triage_status, panel_type_label, role_label, status_source_label, triage_status_label
from .services import NeverMineApiClient, NeverMineApiError
from .services.bridge_client import push_external_event
from .buildmeta import build_runtime_drift_report
from .bot_content import collect_runtime_markers, load_content_snapshot_from_path
from .bot_metrics import build_runtime_metrics_text, increment_runtime_metric, load_persistent_runtime_metrics
from .bot_workflows import build_outbound_comment_payload, targeted_digest_lines, topic_kind_to_event_prefix
from .bot_capabilities import build_capability_report
from .bot_routing import command_surface_policy, routed_interest_aliases, required_subscription_event_kinds
from .bot_scheduler import build_calendar_schedule_payload, build_digest_schedule_payload, build_scheduled_job_dedupe_key, first_calendar_run_at, next_recurring_schedule, recurrence_summary
from .bot_snapshot import restore_capability_sections, snapshot_restore_coverage
from .bot_bridge import build_external_comment_notice, external_comment_attachments, extract_external_comment_id, render_external_comment_body
from .bot_bridge_runtime import _bridge_destination_label, _bridge_destinations, _bridge_destinations_for_event, _bridge_policy_allows, _bridge_semantic_kind, _filter_bridge_payload, queue_bridge_admin_action, queue_bridge_event
from .bot_governance import LEGACY_FLAT_STAFF_COMMANDS, command_alias_map, rules_reacceptance_state
from .bot_extensions import bind_bot_extensions
from .bot_grouped_commands import build_grouped_command_aliases
from .bot_transport import extract_external_content_id, extract_external_topic_id, handled_transport_event_types, transport_contract_coverage_snapshot
from .bot_legacy import legacy_review_summary
from .bot_layout_runtime import (
    _apply_layout_repair,
    _apply_legacy_layout_cleanup,
    _collect_layout_drift,
    _resolve_forum_for_topic,
    _resolve_layout_channel,
    _summarize_layout_drift,
)
from .storage import StorageManager
from .server_layout import ensure_server_layout_file, expected_forum_tags, find_layout_channel, find_layout_role, forum_aliases_by_kind, load_server_layout, validate_server_layout
from .buildmeta import load_build_info

LOGGER = logging.getLogger("nmdiscordbot")
DISCORD_TOP_LEVEL_SLASH_COMMAND_LIMIT = 100
EMBED_COLOR = 0x09ADD3
ERROR_COLOR = 0xD9534F
STAFF_COLOR = 0x8E44AD
AUDIT_COLOR = 0x2ECC71
SENSITIVE_KEY_PATTERN = re.compile(r"(token|secret|password|authorization|api[_-]?key|session|cookie|code)", re.IGNORECASE)


def _sanitize_payload(
    value: Any,
    *,
    max_string_length: int = 256,
    max_collection_items: int = 25,
    max_depth: int = 5,
    _depth: int = 0,
) -> Any:
    if _depth >= max_depth:
        return "***max-depth***"
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        items = list(value.items())[:max_collection_items]
        for key, item in items:
            if SENSITIVE_KEY_PATTERN.search(str(key)):
                sanitized[str(key)] = "***redacted***"
            else:
                sanitized[str(key)] = _sanitize_payload(
                    item,
                    max_string_length=max_string_length,
                    max_collection_items=max_collection_items,
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
        if len(value) > max_collection_items:
            sanitized["__truncated_items__"] = len(value) - max_collection_items
        return sanitized
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        sanitized_items = [
            _sanitize_payload(
                item,
                max_string_length=max_string_length,
                max_collection_items=max_collection_items,
                max_depth=max_depth,
                _depth=_depth + 1,
            )
            for item in items[:max_collection_items]
        ]
        if len(items) > max_collection_items:
            sanitized_items.append(f"***truncated_items:{len(items) - max_collection_items}***")
        return sanitized_items
    if isinstance(value, str):
        return value if len(value) <= max_string_length else value[: max_string_length - 3] + "..."
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    rendered = repr(value)
    return rendered if len(rendered) <= max_string_length else rendered[: max_string_length - 3] + "..."


def _cap_payload_size(value: Any, *, max_bytes: int) -> Any:
    try:
        raw = json.dumps(value, ensure_ascii=False, default=str)
    except TypeError:
        raw = json.dumps(str(value), ensure_ascii=False)
    raw_bytes = raw.encode("utf-8")
    if len(raw_bytes) <= max_bytes:
        return value
    preview_len = max(64, min(max_bytes // 2, 512))
    return {
        "__truncated__": True,
        "preview": raw[:preview_len] + ("..." if len(raw) > preview_len else ""),
        "original_bytes": len(raw_bytes),
    }


def _prepare_audit_payload(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = _sanitize_payload(
        payload,
        max_string_length=settings.audit_payload_max_string_length,
        max_collection_items=settings.audit_payload_max_collection_items,
        max_depth=settings.audit_payload_max_depth,
    )
    capped = _cap_payload_size(sanitized, max_bytes=settings.audit_payload_max_bytes)
    return capped if isinstance(capped, dict) else {"value": capped}


def _preview_payload(value: Any, *, max_length: int) -> str:
    preview = str(value)
    if len(preview) > max_length:
        return preview[: max_length - 3] + "..."
    return preview



def _flatten_content_map(value: Any, *, prefix: str = '') -> dict[str, str]:
    if isinstance(value, dict):
        out: dict[str, str] = {}
        for key, item in value.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            out.update(_flatten_content_map(item, prefix=child))
        return out
    if isinstance(value, list):
        return {prefix: json.dumps(value, ensure_ascii=False, sort_keys=True)}
    return {prefix: str(value)}


def _diff_content_payloads(old_payload: dict[str, Any], new_payload: dict[str, Any]) -> list[str]:
    old_flat = _flatten_content_map(old_payload)
    new_flat = _flatten_content_map(new_payload)
    changes: list[str] = []
    for key in sorted(set(old_flat) | set(new_flat)):
        if old_flat.get(key) != new_flat.get(key):
            changes.append(key)
        if len(changes) >= 20:
            break
    return changes


def _audit_category(action: str) -> str:
    normalized = action.lower()
    if normalized.startswith("verify") or "security" in normalized or normalized.startswith("sync_verified_role"):
        return "security"
    if normalized.startswith("announce") or normalized.startswith("relay_announcement") or normalized.startswith("relay_event"):
        return "business"
    return "ops"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _scope_default_permissions(scope: str) -> discord.Permissions:
    mapping = {
        'support': discord.Permissions(manage_threads=True),
        'content': discord.Permissions(manage_messages=True),
        'events': discord.Permissions(manage_events=True),
        'ops': discord.Permissions(manage_guild=True),
        'bridge': discord.Permissions(manage_guild=True),
        'forum': discord.Permissions(manage_guild=True),
        'topic': discord.Permissions(manage_threads=True),
        'state': discord.Permissions(manage_guild=True),
    }
    return mapping.get(scope, discord.Permissions(manage_guild=True))


def _retry_backoff(attempt: int, *, base_seconds: int, max_seconds: int) -> int:
    attempt = max(1, int(attempt or 1))
    return min(int(max_seconds), max(int(base_seconds), int(base_seconds) * (2 ** (attempt - 1))))


async def _post_runtime_drift_alert(bot: "NMDiscordBot", warnings: list[str], *, resolved: bool = False) -> None:
    now = _utc_now()
    active = await bot.get_runtime_marker('runtime_drift_active') or {}
    fingerprint = hashlib.sha256(json.dumps({'warnings': warnings, 'resolved': resolved}, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
    last_sent = _parse_datetime(str(active.get('last_sent_at') or '')) if isinstance(active, dict) else None
    if not resolved and isinstance(active, dict) and active.get('fingerprint') == fingerprint and last_sent is not None:
        if (now - last_sent).total_seconds() < bot.settings.drift_alert_cooldown_seconds:
            return
    lines = ['Runtime drift устранён.' if resolved else 'Runtime drift обнаружен:']
    if not resolved:
        lines.extend(f'• {item}' for item in warnings[:10])
    payload = {'fingerprint': fingerprint, 'warnings': warnings[:20], 'last_sent_at': _format_dt(now), 'resolved': resolved}
    await bot.set_runtime_marker('runtime_drift_active', payload)
    channels: list[discord.abc.Messageable] = []
    if bot.settings.discord_bot_logs_channel_id:
        channel = bot._get_message_channel(bot.settings.discord_bot_logs_channel_id)
        if channel is not None:
            channels.append(channel)
    guild = bot.get_guild(bot.settings.discord_guild_id) if bot.settings.discord_guild_id else None
    if guild is not None:
        for alias in ('staff_briefing', 'bot_logs'):
            channel = _resolve_layout_channel(bot, guild, alias)
            if channel is not None and isinstance(channel, discord.abc.Messageable) and channel not in channels:
                channels.append(channel)
    for channel in channels[:2]:
        with contextlib.suppress(Exception):
            await channel.send('\n'.join(lines)[:1900])


def _classify_bridge_error(raw: str | None) -> str:
    value = (raw or '').strip().lower()
    if not value:
        return 'неизвестно'
    if any(word in value for word in ('401', '403', 'unauthorized', 'forbidden', 'signature', 'auth')):
        return 'аутентификация'
    if any(word in value for word in ('timeout', 'timed out', 'deadline')):
        return 'таймаут'
    if any(word in value for word in ('rate limit', '429', 'too many requests')):
        return 'rate-limit'
    if any(word in value for word in ('validation', 'bad request', '400', 'unprocessable', '422')):
        return 'валидация'
    if any(word in value for word in ('connection', 'dns', 'network', 'unreachable', 'reset')):
        return 'сеть'
    if any(word in value for word in ('500', '502', '503', '504', 'remote')):
        return 'удалённая сторона'
    return 'прочее'


def _format_timedelta_seconds(seconds: float | int) -> str:
    total = int(max(0, seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts=[]
    if days:
        parts.append(f"{days}д")
    if hours:
        parts.append(f"{hours}ч")
    if minutes:
        parts.append(f"{minutes}м")
    if secs or not parts:
        parts.append(f"{secs}с")
    return ' '.join(parts)
class TopicActionsView(discord.ui.View):
    def __init__(self, bot: "NMDiscordBot", thread_id: int) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        self.thread_id = thread_id

    async def _get_thread(self, interaction: discord.Interaction) -> discord.Thread | None:
        if isinstance(interaction.channel, discord.Thread) and interaction.channel.id == self.thread_id:
            return interaction.channel
        return await self.bot._resolve_thread(str(self.thread_id))

    async def _change_status(self, interaction: discord.Interaction, status: str) -> None:
        thread = await self._get_thread(interaction)
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        if not self.bot.has_staff_scope(interaction.user, 'support'):
            await interaction.response.send_message("Недостаточно прав для работы с темой.", ephemeral=True)
            return
        record = await self.bot.community_store.get_forum_topic(str(thread.id))
        topic_kind = str((record or {}).get('topic_kind') or 'support')
        metadata = dict((record or {}).get('metadata_json') or {})
        metadata['last_staff_response_at'] = _format_dt(_utc_now())
        metadata['last_staff_response_by'] = str(interaction.user.id)
        metadata['last_staff_response_name'] = str(interaction.user)
        await self.bot.apply_thread_status(thread=thread, topic_kind=topic_kind, status=status, metadata=metadata)
        await interaction.response.send_message(f"Статус темы обновлён: {status}.", ephemeral=True)

    @discord.ui.button(label="Взять тему", style=discord.ButtonStyle.primary, custom_id="nmdiscord:topic:claim")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        thread = await self._get_thread(interaction)
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        if not self.bot.has_staff_scope(interaction.user, 'support'):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        await self.bot.community_store.assign_forum_topic_owner(thread_id=str(thread.id), staff_user_id=str(interaction.user.id), staff_name=str(interaction.user))
        await self.bot.notify_topic_owner_change(thread=thread, new_owner_user_id=str(interaction.user.id), actor_name=str(interaction.user))
        await interaction.response.send_message("Тема назначена на вас.", ephemeral=True)

    @discord.ui.button(label="На рассмотрении", style=discord.ButtonStyle.secondary, custom_id="nmdiscord:topic:review")
    async def review(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._change_status(interaction, 'in_review')

    @discord.ui.button(label="Решено", style=discord.ButtonStyle.success, custom_id="nmdiscord:topic:resolved")
    async def resolved(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._change_status(interaction, 'resolved')

    @discord.ui.button(label="Закрыть", style=discord.ButtonStyle.danger, custom_id="nmdiscord:topic:closed")
    async def closed(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._change_status(interaction, 'closed')

    @discord.ui.button(label="Экспорт", style=discord.ButtonStyle.secondary, custom_id="nmdiscord:topic:export")
    async def export(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        thread = await self._get_thread(interaction)
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        filename, payload = await self.bot.build_thread_transcript(thread, mode='staff')
        await interaction.response.send_message("Транскрипт темы сформирован.", file=discord.File(io.BytesIO(payload), filename=filename), ephemeral=True)


class TopicCreateModal(discord.ui.Modal):
    def __init__(self, cog: "CommunityCommands", *, topic_kind: str, forum_channel_id: int | None, title: str) -> None:
        super().__init__(title=title)
        self.cog = cog
        self.topic_kind = topic_kind
        self.forum_channel_id = forum_channel_id
        self.summary = discord.ui.TextInput(label="Заголовок", max_length=100)
        self.details = discord.ui.TextInput(label="Описание", style=discord.TextStyle.paragraph, max_length=1800)
        self.add_item(self.summary)
        self.add_item(self.details)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        mapping = {
            'support': ('support_topic_general', 'community.support.created', 'support', {'area': 'general'}),
            'bug': ('bug_report_created', 'community.bug_report.created', 'bug', None),
            'suggestion': ('suggestion_created', 'community.suggestion.created', 'suggestion', None),
            'appeal': ('appeal_created', 'community.appeal.created', 'appeal', None),
        }
        audit_action, bridge_event_kind, topic_kind, extra = mapping.get(self.topic_kind, mapping['support'])
        thread, error = await self.cog._create_forum_topic(
            interaction=interaction,
            forum_channel_id=self.forum_channel_id,
            title=str(self.summary.value),
            body=str(self.details.value),
            audit_action=audit_action,
            bridge_event_kind=bridge_event_kind,
            topic_kind=topic_kind,
            extra_payload=extra,
        )
        if error:
            await interaction.response.send_message(error, ephemeral=True)
        else:
            await interaction.response.send_message(f"Тема создана: {thread.mention if thread else 'готово'}", ephemeral=True)

class CommunityCommands(commands.Cog):
    def __init__(self, bot: "NMDiscordBot") -> None:
        self.bot = bot
        self.settings = bot.settings
        self.api = bot.api
        self.storage = bot.storage
        self.community_store = bot.community_store

    def _channel_ref(self, channel_id: int | None) -> str:
        return f"<#{channel_id}>" if channel_id else "не настроено"

    async def _enforce_command_cooldown(self, interaction: discord.Interaction, name: str, *, seconds: int | None = None) -> bool:
        ttl = await self.storage.command_cooldown(discord_user_id=interaction.user.id, command_name=f"{name}:{seconds or self.settings.community_command_cooldown_seconds}")
        if ttl > 0:
            await interaction.response.send_message(f"Подожди {ttl} сек. перед повторным использованием команды.", ephemeral=True)
            return False
        return True

    def _forum_policy(self, topic_kind: str) -> dict[str, Any]:
        defaults = {
            'auto_close_after_seconds': self.settings.forum_recruitment_auto_close_hours * 3600 if topic_kind == 'guild_recruitment' else self.settings.forum_auto_close_inactive_hours * 3600,
            'escalation_hours': {
                'support': self.settings.support_escalation_hours,
                'appeal': self.settings.appeal_escalation_hours,
                'report': self.settings.report_escalation_hours,
                'guild_recruitment': self.settings.forum_recruitment_auto_close_hours,
                'chronicle': self.settings.support_escalation_hours,
                'lore_discussion': self.settings.support_escalation_hours,
            }.get(topic_kind, self.settings.support_escalation_hours),
            'export_mode': 'auto',
        }
        override = self.settings.forum_policy_overrides.get(topic_kind, {}) if self.settings.forum_policy_overrides else {}
        for key in ('auto_close_after_seconds', 'escalation_hours'):
            if key in override:
                try:
                    defaults[key] = int(override[key])
                except Exception:
                    pass
        if 'export_mode' in override and str(override.get('export_mode') or '').strip():
            defaults['export_mode'] = str(override.get('export_mode')).strip().lower()
        return defaults

    def _attachment_section(self, attachment: discord.Attachment | None) -> tuple[str, dict[str, Any]]:
        if attachment is None:
            return '', {}
        return f"\n\n**Вложение:** [{attachment.filename}]({attachment.url})", {'attachment_url': attachment.url, 'attachment_filename': attachment.filename, 'attachment_size': attachment.size}

    def _attachment_policy(self, topic_kind: str) -> dict[str, Any]:
        default = {
            'max_bytes': int(self.settings.attachment_max_bytes_default),
            'allowed_extensions': tuple(self.settings.attachment_allowed_extensions_default),
            'blocked_extensions': tuple(self.settings.attachment_blocked_extensions_default),
        }
        raw = dict(self.settings.forum_attachment_policy.get(topic_kind, {}) or {})
        if 'max_bytes' in raw:
            with contextlib.suppress(Exception):
                default['max_bytes'] = int(raw['max_bytes'])
        if 'allowed_extensions' in raw:
            vals = raw['allowed_extensions']
            if isinstance(vals, str):
                vals = vals.split(',')
            default['allowed_extensions'] = tuple(str(v).strip().lower() for v in vals if str(v).strip())
        if 'blocked_extensions' in raw:
            vals = raw['blocked_extensions']
            if isinstance(vals, str):
                vals = vals.split(',')
            default['blocked_extensions'] = tuple(str(v).strip().lower() for v in vals if str(v).strip())
        return default

    def _validate_attachment(self, topic_kind: str, attachment: discord.Attachment | None) -> str | None:
        if attachment is None:
            return None
        policy = self._attachment_policy(topic_kind)
        filename = (attachment.filename or '').strip()
        ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        blocked = {x.lower() for x in policy.get('blocked_extensions', ()) if x}
        allowed = {x.lower() for x in policy.get('allowed_extensions', ()) if x}
        if ext and ext in blocked:
            return f'Файлы с расширением .{ext} запрещены для этой темы.'
        if allowed and ext and ext not in allowed:
            return f'Для этой темы разрешены только файлы: {", ".join(sorted(allowed))}.'
        max_bytes = int(policy.get('max_bytes') or 0)
        if max_bytes > 0 and int(getattr(attachment, 'size', 0) or 0) > max_bytes:
            return f'Вложение слишком большое. Лимит: {max_bytes} байт.'
        return None

    def _iter_attachments(self, *attachments: discord.Attachment | None) -> list[discord.Attachment]:
        unique: list[discord.Attachment] = []
        seen: set[int] = set()
        for item in attachments:
            if item is None:
                continue
            aid = int(getattr(item, 'id', 0) or 0)
            if aid and aid in seen:
                continue
            if aid:
                seen.add(aid)
            unique.append(item)
        return unique[: max(1, self.settings.max_topic_attachments)]

    def _attachments_section(self, attachments: list[discord.Attachment]) -> tuple[str, dict[str, Any]]:
        if not attachments:
            return '', {}
        lines = []
        items = []
        total = 0
        for attachment in attachments[: max(1, self.settings.max_topic_attachments)]:
            lines.append(f"- [{attachment.filename}]({attachment.url})")
            items.append({
                'filename': attachment.filename,
                'url': attachment.url,
                'size': int(getattr(attachment, 'size', 0) or 0),
                'content_type': str(getattr(attachment, 'content_type', '') or ''),
            })
            total += int(getattr(attachment, 'size', 0) or 0)
        return "\n\n**Вложения:**\n" + "\n".join(lines), {'attachments': items, 'attachments_total_bytes': total}

    def _validate_attachments(self, topic_kind: str, attachments: list[discord.Attachment]) -> str | None:
        if len(attachments) > self.settings.max_topic_attachments:
            return f'Для этой темы можно приложить не более {self.settings.max_topic_attachments} вложений.'
        policy = self._attachment_policy(topic_kind)
        total = sum(int(getattr(item, 'size', 0) or 0) for item in attachments)
        total_limit = int(getattr(self.settings, 'attachment_total_max_bytes_default', 0) or 0)
        if total_limit > 0 and total > total_limit:
            return f'Суммарный размер вложений превышает лимит {total_limit} байт.'
        for attachment in attachments:
            error = self._validate_attachment(topic_kind, attachment)
            if error:
                return error
        return None

    def _resolve_target_thread_id(self, interaction: discord.Interaction, thread_id: str | None) -> str | None:
        if thread_id and str(thread_id).strip():
            return str(thread_id).strip()
        if isinstance(interaction.channel, discord.Thread):
            return str(interaction.channel.id)
        return None


    async def _autocomplete_thread_id(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        rows = await self.community_store.list_forum_topics(limit=20)
        result=[]
        current_l=(current or '').lower()
        for row in rows:
            label=f"{row.get('thread_id')} • {row.get('title') or row.get('topic_kind') or 'тема'}"
            if current_l and current_l not in label.lower():
                continue
            result.append(app_commands.Choice(name=label[:100], value=str(row.get('thread_id'))))
        return result[:25]

    async def _autocomplete_job_id(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[int]]:
        rows = await self.community_store.list_scheduled_jobs(limit=20)
        result=[]
        current_l=(current or '').lower()
        for row in rows:
            label=f"#{row.get('id')} • {row.get('job_type')} • {row.get('status')}"
            if current_l and current_l not in label.lower():
                continue
            result.append(app_commands.Choice(name=label[:100], value=int(row.get('id'))))
        return result[:25]

    async def _autocomplete_event_id(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[int]]:
        rows = await self.community_store.list_failed_external_sync_events(limit=20)
        result=[]
        current_l=(current or '').lower()
        for row in rows:
            label=f"#{row.get('id')} • {row.get('destination') or '—'} • {row.get('event_kind') or '—'}"
            if current_l and current_l not in label.lower():
                continue
            result.append(app_commands.Choice(name=label[:100], value=int(row.get('id'))))
        return result[:25]

    async def _autocomplete_request_id(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[int]]:
        rows = await self.community_store.list_approval_requests(limit=20)
        result=[]
        current_l=(current or '').lower()
        for row in rows:
            label=f"#{row.get('id')} • {row.get('kind')} • {row.get('status')}"
            if current_l and current_l not in label.lower():
                continue
            result.append(app_commands.Choice(name=label[:100], value=int(row.get('id'))))
        return result[:25]

    async def _autocomplete_topic_kind(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        values = ['support', 'bug', 'suggestion', 'appeal', 'guild_recruitment', 'report', 'chronicle', 'lore_discussion']
        current_l = (current or '').lower()
        return [app_commands.Choice(name=v, value=v) for v in values if not current_l or current_l in v][:25]

    async def _autocomplete_destination(self, interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        values = [str(v) for v in self.bot._bridge_destinations()] or ['telegram', 'vk', 'community_core', 'workspace']
        current_l = (current or '').lower()
        return [app_commands.Choice(name=v, value=v) for v in values if not current_l or current_l in v][:25]

    async def _notify_topic_owner(self, *, thread: discord.Thread | None, owner_user_id: str | None, text: str) -> None:
        if not owner_user_id or thread is None:
            return
        try:
            await thread.send(f"<@{owner_user_id}> {text}")
        except Exception:
            LOGGER.exception('Failed to notify topic owner for thread %s', getattr(thread, 'id', None))

    async def _build_thread_transcript(self, thread: discord.Thread, mode: str = 'staff') -> tuple[str, bytes]:
        normalized_mode = (mode or 'staff').strip().lower()
        lines = [f"Транскрипт темы #{thread.name}", f"thread_id={thread.id}", f"mode={normalized_mode}", ""]
        async for message in thread.history(limit=self.settings.topic_transcript_history_limit, oldest_first=True):
            created = message.created_at.replace(microsecond=0).isoformat() if message.created_at else '—'
            author = getattr(message.author, 'display_name', None) or getattr(message.author, 'name', 'unknown')
            if normalized_mode == 'metadata':
                lines.append(f"[{created}] {author}: сообщение")
                lines.append('')
                continue
            lines.append(f"[{created}] {author} ({message.author.id}):")
            content = (message.content or '').strip() or '[без текста]'
            if normalized_mode == 'public' and message.author.bot:
                content = '[скрыто в публичном экспорте]'
            lines.append(content)
            if normalized_mode != 'public':
                for attachment in message.attachments:
                    lines.append(f"  вложение: {attachment.filename} -> {attachment.url}")
            lines.append('')
        return f"thread-{thread.id}-transcript.txt", '\n'.join(lines).encode('utf-8')

    def _forum_template(self, topic_kind: str, title: str, body: str, extra: dict[str, Any] | None = None) -> str:
        extra = extra or {}
        title = title.strip()
        body = body.strip()
        template = get_topic_template(self.settings, topic_kind)
        if template:
            requirements = str(extra.get('requirements') or 'Не указаны')
            attachments_hint = str(extra.get('attachments_hint') or 'При необходимости приложите скриншот, лог или файл.')
            with contextlib.suppress(Exception):
                return template.format(title=title, body=body, requirements=requirements, attachments_hint=attachments_hint)
        return body


    def _ops_embed(self, title: str, description: str | None = None) -> discord.Embed:
        embed = discord.Embed(title=title, description=description or None, color=STAFF_COLOR)
        embed.set_footer(text=f"NMDiscordBot {self.bot.version}")
        return embed

    async def _queue_risky_approval(self, interaction: discord.Interaction, *, kind: str, payload: dict[str, Any], summary: str) -> int:
        approval_policy = 'quorum' if kind in {'state_restore', 'layout_repair', 'legacy_layout_cleanup'} else 'single_admin'
        required_approvals = 2 if approval_policy == 'quorum' else 1
        request_id = await self.community_store.create_approval_request(
            kind=kind,
            payload=payload,
            requested_by=str(interaction.user.id),
            requested_by_name=str(interaction.user),
            required_role='admin',
            expires_in_seconds=24 * 3600,
            required_approvals=required_approvals,
            approval_policy=approval_policy,
        )
        await self.bot.record_audit(action=f'{kind}_approval_requested', actor_user_id=interaction.user.id, target_user_id=None, status='pending', payload={'request_id': request_id, 'summary': summary, 'kind': kind, 'approval_policy': approval_policy, 'required_approvals': required_approvals})
        return request_id

    async def _execute_approval_payload(self, kind: str, payload: dict[str, Any], *, approver_user_id: int) -> str:
        normalized = str(kind or '').strip().lower()
        if normalized == 'layout_repair':
            guild_id = int(str(payload.get('guild_id') or '0') or 0)
            guild = self.bot.get_guild(guild_id) if guild_id else None
            if guild is None:
                return 'сервер недоступен'
            drift = await _collect_layout_drift(self.bot, guild)
            fixes = await _apply_layout_repair(self.bot, guild, drift, str(payload.get('scope') or 'all'))
            await self.bot._reconcile_panels(guild.id)
            await self.bot.record_audit(action='layout_repair_approved', actor_user_id=approver_user_id, target_user_id=None, status='success', payload={'scope': payload.get('scope') or 'all', 'fixes': fixes})
            return ', '.join(fixes) if fixes else 'изменения не потребовались'
        if normalized == 'state_restore':
            restored = await _apply_state_restore_payload(self, payload.get('snapshot') if isinstance(payload.get('snapshot'), dict) else {}, section=str(payload.get('section') or 'all'), guild_id=str(payload.get('guild_id') or ''), actor_user_id=approver_user_id)
            return ', '.join(restored) if restored else 'ничего не изменено'
        if normalized == 'bridge_dead_letter_requeue':
            destination = str(payload.get('destination') or '') or None
            event_kind = str(payload.get('event_kind') or '') or None
            limit = int(payload.get('limit') or 10)
            hours = int(payload.get('hours') or 24)
            rows = await self.community_store.list_failed_external_sync_events(limit=limit, destination=destination, event_kind=event_kind, since_hours=hours)
            retried = 0
            for row in rows:
                if await self.community_store.requeue_external_sync_event(int(row.get('id') or 0)):
                    retried += 1
            await self.bot.record_audit(action='bridge_dead_letter_requeue_approved', actor_user_id=approver_user_id, target_user_id=None, status='success', payload={'destination': destination or '', 'event_kind': event_kind or '', 'hours': hours, 'limit': limit, 'retried': retried})
            return f'повторно поставлено: {retried}'
        if normalized == 'legacy_layout_cleanup':
            guild_id = int(str(payload.get('guild_id') or '0') or 0)
            guild = self.bot.get_guild(guild_id) if guild_id else None
            if guild is None:
                return 'сервер недоступен'
            actions = await _apply_legacy_layout_cleanup(self.bot, guild, limit=int(payload.get('limit') or 20))
            await self.bot.record_audit(action='legacy_layout_cleanup_approved', actor_user_id=approver_user_id, target_user_id=None, status='success', payload={'limit': int(payload.get('limit') or 20), 'actions': actions[:20]})
            return ', '.join(actions) if actions else 'удалять нечего'
        return 'операция не поддерживается'

    def _help_topic_embed(self, topic: str) -> discord.Embed:
        topic = normalize_help_topic(topic)
        embed = discord.Embed(title=f"Навигация NeverMine / {topic}", color=EMBED_COLOR)
        mapping = {
            'старт': ("С чего начать", f"Начни с {self._channel_ref(self.settings.discord_start_here_channel_id)} и прочитай {self._channel_ref(self.settings.discord_rules_channel_id)}. После этого используй панель входа, чтобы получить роль участника."),
            'правила': ("Правила", f"Основные правила находятся в {self._channel_ref(self.settings.discord_rules_channel_id)}."),
            'роли': ("Роли и доступ", f"Роли интересов и доступ находятся в {self._channel_ref(self.settings.discord_roles_channel_id)}."),
            'вопросы': ("Частые вопросы", f"Частые вопросы собраны в {self._channel_ref(self.settings.discord_faq_channel_id)}."),
            'поддержка': ("Поддержка", f"Используй {self._channel_ref(self.settings.discord_forum_help_channel_id)}, {self._channel_ref(self.settings.discord_forum_launcher_and_tech_channel_id)} и {self._channel_ref(self.settings.discord_forum_account_help_channel_id)}."),
            'баги': ("Баги", f"О баге лучше писать в {self._channel_ref(self.settings.discord_forum_bug_reports_channel_id)}."),
            'предложения': ("Предложения", f"Предложения собираются в {self._channel_ref(self.settings.discord_forum_suggestions_channel_id)}."),
            'гильдии': ("Гильдии", f"Набор в гильдии — в {self._channel_ref(self.settings.discord_forum_guild_recruitment_channel_id)}."),
            'события': ("События", f"Следи за {self._channel_ref(self.settings.discord_events_channel_id)} и stage-каналом {self._channel_ref(self.settings.discord_stage_channel_id)}."),
            'апелляции': ("Апелляции", f"Апелляции подаются в {self._channel_ref(self.settings.discord_forum_appeals_channel_id)}."),
        }
        title, description = mapping.get(topic, mapping['старт'])
        embed.title = title
        embed.description = description
        return embed


    async def _require_scope(self, interaction: discord.Interaction, scope: str) -> bool:
        if self.bot.has_staff_scope(interaction.user, scope):
            return True
        await interaction.response.send_message("Недостаточно прав для этого раздела.", ephemeral=True)
        return False

    async def _targeted_digest_mentions(self, *, digest_kind: str, guild: discord.Guild | None) -> str:
        if guild is None:
            return ''
        rows = await self.community_store.list_matching_subscription_targets(platform='discord', digest_kind=digest_kind, limit=250)
        mentions: list[str] = []
        for row in rows:
            user_id = str(row.get('platform_user_id') or '').strip()
            if not user_id.isdigit():
                continue
            member = guild.get_member(int(user_id))
            if member is None:
                continue
            mentions.append(member.mention)
        return ' '.join(dict.fromkeys(mentions))

    async def _send_targeted_digest(self, *, digest_kind: str, channel: discord.abc.Messageable, guild: discord.Guild | None, actor_user_id: int | None = None) -> bool:
        overdue_support = await self.community_store.list_topics_needing_escalation(topic_kind='support', older_than_hours=self.settings.support_escalation_hours, limit=20)
        overdue_appeals = await self.community_store.list_topics_needing_escalation(topic_kind='appeal', older_than_hours=self.settings.appeal_escalation_hours, limit=20)
        failed_bridge = await self.community_store.list_failed_external_sync_events(limit=20, since_hours=24)
        rules_outdated = await self.community_store.list_rules_reacceptance_candidates(guild_id=str(getattr(guild, 'id', self.settings.discord_guild_id or '')), current_rules_version=self.settings.rules_version, limit=100)
        stale_approvals = await self.community_store.list_expired_pending_approval_requests(limit=100)
        lines = targeted_digest_lines(
            digest_kind=digest_kind,
            overdue_support=overdue_support,
            overdue_appeals=overdue_appeals,
            failed_bridge=failed_bridge,
            rules_outdated_count=len(rules_outdated),
            stale_approvals=len(stale_approvals),
        )
        if not lines:
            return False
        mentions = await self._targeted_digest_mentions(digest_kind=digest_kind, guild=guild)
        embed = discord.Embed(title=f'Тематическая staff-сводка / {digest_kind}', color=STAFF_COLOR, description='\n'.join(f'• {line}' for line in lines))
        message = await channel.send(content=mentions or None, embed=embed)
        await self.community_store.upsert_external_content_mirror(source_platform='discord', content_kind='announcement', external_content_id=str(message.id), discord_channel_id=str(getattr(channel, 'id', '') or ''), discord_message_id=str(message.id), metadata={'origin': 'discord-command'})
        await self.bot.record_audit(action='targeted_digest_sent', actor_user_id=actor_user_id, target_user_id=None, status='success', payload={'digest_kind': digest_kind, 'summary': lines, 'mentions': mentions.count('<@')})
        return True

    async def _bridge_thread_comment(self, message: discord.Message, *, action: str) -> None:
        if message.guild is None or isinstance(message.author, discord.ClientUser) or message.author.bot:
            return
        if not isinstance(message.channel, discord.Thread):
            return
        topic = await self.community_store.get_forum_topic(str(message.channel.id))
        if topic is None:
            return
        metadata = dict(topic.get('metadata_json') or {})
        dedupe_key = {
            'comment.appended': 'last_bridge_comment_message_id',
            'comment.edited': 'last_bridge_comment_edit_message_id',
            'comment.deleted': 'last_bridge_comment_delete_message_id',
        }.get(action, 'last_bridge_comment_message_id')
        if str(metadata.get(dedupe_key) or '') == str(message.id):
            return
        if action == 'comment.appended' and not (message.content or message.attachments):
            return
        event_kind, payload = build_outbound_comment_payload(message, topic, action=action)
        if action == 'comment.edited':
            payload['edited_at'] = _format_dt(_utc_now())
        if action == 'comment.deleted':
            payload['deleted_at'] = _format_dt(_utc_now())
        if not payload.get('comment') and not payload.get('attachments') and action == 'comment.appended':
            return
        await self.bot.queue_bridge_event(event_kind, payload)
        metadata[dedupe_key] = str(message.id)
        metadata['last_bridge_comment_author_id'] = str(message.author.id)
        metadata['last_bridge_comment_action'] = action
        metadata['last_bridge_comment_at'] = _format_dt(_utc_now())
        await self.community_store.update_forum_topic_state(thread_id=str(message.channel.id), status=str(topic.get('status') or 'open'), tags=list(topic.get('tags_json') or []), metadata=metadata, closed=bool(topic.get('closed_at')))

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        await self._bridge_thread_comment(message, action='comment.appended')

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message) -> None:
        if after.id != before.id:
            return
        if str(before.content or '') == str(after.content or '') and len(before.attachments) == len(after.attachments):
            return
        await self._bridge_thread_comment(after, action='comment.edited')

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message) -> None:
        await self._bridge_thread_comment(message, action='comment.deleted')

    @app_commands.command(name="rules_reacceptance_status", description="Показать, кому нужно повторно принять правила")
    @app_commands.default_permissions(manage_guild=True)
    async def rules_reacceptance_status(self, interaction: discord.Interaction, limit: app_commands.Range[int,1,50] = 10) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        guild_id = str(interaction.guild_id or self.settings.discord_guild_id or '')
        rows = await self.community_store.list_rules_reacceptance_candidates(guild_id=guild_id, current_rules_version=self.settings.rules_version, limit=limit)
        stats = await self.community_store.get_rules_acceptance_stats(guild_id=guild_id, current_rules_version=self.settings.rules_version)
        embed = self._ops_embed('Повторное принятие правил')
        embed.add_field(name='Текущая версия правил', value=self.settings.rules_version, inline=True)
        embed.add_field(name='Всего записей', value=str(stats.get('total') or 0), inline=True)
        embed.add_field(name='Актуальная версия', value=str(stats.get('current_version') or 0), inline=True)
        if rows:
            preview = []
            for row in rows[:10]:
                preview.append(f"<@{row.get('discord_user_id')}>: {row.get('accepted_rules_version') or '—'} → {self.settings.rules_version}")
            embed.description = '\n'.join(preview)
        else:
            embed.description = 'Все участники с записью в журнале уже приняли актуальную версию правил.'
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="rules_reacceptance_nudge", description="Отправить напоминание о повторном принятии правил")
    @app_commands.default_permissions(manage_guild=True)
    async def rules_reacceptance_nudge(self, interaction: discord.Interaction, limit: app_commands.Range[int,1,25] = 10) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        guild_id = str(interaction.guild_id or self.settings.discord_guild_id or '')
        rows = await self.community_store.list_rules_reacceptance_candidates(guild_id=guild_id, current_rules_version=self.settings.rules_version, limit=limit)
        if not rows:
            await interaction.response.send_message('Нет участников, которым требуется повторно принять правила.', ephemeral=True)
            return
        mentions = ' '.join(f"<@{row.get('discord_user_id')}>" for row in rows if str(row.get('discord_user_id') or '').isdigit())
        channel = interaction.channel if isinstance(interaction.channel, discord.abc.Messageable) else None
        if channel is not None and mentions:
            await channel.send(f"{mentions}\nПожалуйста, повторно примите актуальную версию правил NeverMine через панель входа. Текущая версия: `{self.settings.rules_version}`.")
        await self.bot.record_audit(action='rules_reacceptance_nudged', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'count': len(rows), 'rules_version': self.settings.rules_version})
        await interaction.response.send_message(f'Напоминание отправлено для {len(rows)} участников.', ephemeral=True)

    @app_commands.command(name="targeted_digest_now", description="Отправить тематическую staff-сводку по подпискам")
    @app_commands.default_permissions(manage_guild=True)
    async def targeted_digest_now(self, interaction: discord.Interaction, digest_kind: str = 'staff', channel_id: str | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        destination = interaction.channel
        if channel_id and channel_id.isdigit():
            destination = self.bot._get_message_channel(int(channel_id)) or interaction.channel
        sent = await self._send_targeted_digest(digest_kind=digest_kind, channel=destination, guild=interaction.guild, actor_user_id=interaction.user.id)
        await interaction.response.send_message('Тематическая сводка отправлена.' if sent else 'Для выбранного типа сводка сейчас пуста.', ephemeral=True)

    @app_commands.command(name="targeted_digest_schedule", description="Запланировать тематическую staff-сводку")
    @app_commands.default_permissions(manage_guild=True)
    async def targeted_digest_schedule(self, interaction: discord.Interaction, digest_kind: str = 'staff', hours_from_now: app_commands.Range[int,1,168] = 24, channel_id: str | None = None, repeat_every_hours: int = 0, repeat_count: int = 0) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        if repeat_every_hours < 0 or repeat_every_hours > 168 or repeat_count < 0 or repeat_count > 365:
            await interaction.response.send_message('repeat_every_hours должен быть в диапазоне 0..168, repeat_count — 0..365.', ephemeral=True)
            return
        dt = _utc_now() + timedelta(hours=int(hours_from_now))
        target_channel_id = str(channel_id or getattr(interaction.channel, 'id', '') or '')
        payload = build_digest_schedule_payload(digest_kind=digest_kind, recurrence_hours=repeat_every_hours or None, remaining_occurrences=repeat_count or None, digest_scope='targeted')
        run_at = _format_dt(dt)
        dedupe_key = build_scheduled_job_dedupe_key(job_type='targeted_digest', guild_id=str(interaction.guild_id or ''), channel_id=target_channel_id, run_at=run_at, payload=payload)
        job_id = await self.community_store.schedule_job(job_type='targeted_digest', run_at=run_at, payload=payload, guild_id=str(interaction.guild_id or ''), channel_id=target_channel_id, created_by=str(interaction.user.id), dedupe_key=dedupe_key)
        await self.bot.record_audit(action='targeted_digest_scheduled', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'job_id': job_id, 'run_at': run_at, 'channel_id': target_channel_id, 'digest_kind': digest_kind, 'recurrence': recurrence_summary(recurrence_hours=repeat_every_hours, remaining_occurrences=repeat_count or None)})
        await interaction.response.send_message(f'Тематическая сводка `{digest_kind}` запланирована на {run_at} ({recurrence_summary(recurrence_hours=repeat_every_hours, remaining_occurrences=repeat_count or None)}).', ephemeral=True)

    @app_commands.command(name="targeted_digest_calendar", description="Запланировать тематическую сводку по календарю")
    @app_commands.default_permissions(manage_guild=True)
    async def targeted_digest_calendar(self, interaction: discord.Interaction, digest_kind: str = 'staff', local_time: str = '09:00', weekday: str | None = None, timezone_name: str = 'Europe/Berlin', channel_id: str | None = None, repeat_count: int = 0, weekday_set: str | None = None, day_of_month: app_commands.Range[int,1,28] | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        if repeat_count < 0 or repeat_count > 365:
            await interaction.response.send_message('repeat_count должен быть в диапазоне 0..365.', ephemeral=True)
            return
        target_channel_id = str(channel_id or getattr(interaction.channel, 'id', '') or '')
        payload = build_calendar_schedule_payload(digest_kind=digest_kind, digest_scope='targeted', timezone_name=timezone_name, local_time=local_time, weekday=weekday or None, weekday_set=weekday_set or None, day_of_month=int(day_of_month) if day_of_month else None, remaining_occurrences=repeat_count or None)
        run_at = first_calendar_run_at(payload=payload)
        dedupe_key = build_scheduled_job_dedupe_key(job_type='targeted_digest', guild_id=str(interaction.guild_id or ''), channel_id=target_channel_id, run_at=run_at, payload=payload)
        job_id = await self.community_store.schedule_job(job_type='targeted_digest', run_at=run_at, payload=payload, guild_id=str(interaction.guild_id or ''), channel_id=target_channel_id, created_by=str(interaction.user.id), dedupe_key=dedupe_key)
        summary = recurrence_summary(calendar_mode=str(payload.get('calendar_mode') or ''), calendar_time=str(payload.get('calendar_time') or ''), calendar_weekday=str(payload.get('calendar_weekday') or ''), calendar_weekdays=list(payload.get('calendar_weekdays') or []), calendar_day_of_month=int(payload.get('calendar_day_of_month') or 0) or None, timezone_name=str(payload.get('calendar_timezone') or ''), remaining_occurrences=repeat_count or None)
        await self.bot.record_audit(action='targeted_digest_calendar_scheduled', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'job_id': job_id, 'run_at': run_at, 'channel_id': target_channel_id, 'digest_kind': digest_kind, 'recurrence': summary})
        await interaction.response.send_message(f'Тематическая сводка `{digest_kind}` запланирована на {run_at} ({summary}).', ephemeral=True)

    @app_commands.command(name="capability_report", description="Показать capability self-report runtime")
    @app_commands.default_permissions(manage_guild=True)
    async def capability_report(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        build_info = load_build_info()
        all_commands = self.bot.tree.get_commands()
        grouped_count = len([cmd for cmd in all_commands if isinstance(cmd, app_commands.Group)])
        flat_count = len([cmd for cmd in all_commands if isinstance(cmd, app_commands.Command)])
        schema_version = await self.community_store.get_schema_version()
        surface_policy = command_surface_policy(grouped_count=grouped_count, flat_count=flat_count, mode=self.settings.command_surface_mode, removed_flat_aliases=getattr(self.bot, '_removed_flat_aliases', []), missing_permission_gates=getattr(self.bot, '_missing_permission_gates', []))
        migrations = await self.community_store.list_community_schema_migrations(limit=200)
        migration_plan = await self.community_store.schema_migration_plan()
        alias_map = command_alias_map()
        restore_capabilities = restore_capability_sections()
        contract_coverage = transport_contract_coverage_snapshot()
        contract_coverage['routing_without_validator'] = sorted(event_kind for event_kind in required_subscription_event_kinds() if event_kind not in set(declared_transport_event_types()))
        contract_coverage['routing_without_handler'] = sorted(event_kind for event_kind in required_subscription_event_kinds() if event_kind not in set(handled_transport_event_types()))
        runtime_hooks = {
            'rules_reacceptance_loop_bound': callable(getattr(self.bot, '_rules_reacceptance_loop', None)),
            'comment_mirror_runtime_bound': callable(getattr(self.community_store, 'upsert_bridge_comment_mirror', None)) and callable(getattr(self.bot, 'handle_incoming_transport_event', None)) and callable(getattr(self.bot, '_bridge_thread_comment', None)),
            'discussion_mirror_registry_bound': callable(getattr(self.community_store, 'upsert_external_discussion_mirror', None)),
            'content_mirror_registry_bound': callable(getattr(self.community_store, 'upsert_external_content_mirror', None)),
            'legacy_lifecycle_bound': callable(getattr(self.community_store, 'upsert_legacy_layout_resource', None)),
            'state_restore_replay_bound': callable(globals().get('_apply_state_restore_payload')),
            'group_alias_map_loaded': bool(alias_map),
            'recurring_digest_schedule_supported': callable(getattr(self, 'targeted_digest_schedule', None)) and callable(getattr(self, 'staff_digest_schedule', None)),
            'calendar_digest_schedule_supported': callable(getattr(self, 'targeted_digest_calendar', None)) and callable(getattr(self, 'staff_digest_calendar', None)),
            'subscription_route_coverage': {event_kind: bool(routed_interest_aliases(event_kind)) for event_kind in required_subscription_event_kinds()},
            'migration_plan_in_sync': bool(migration_plan.get('schema_version_matches_expected')),
        }
        report = build_capability_report(
            runtime_version=self.bot.version,
            build_info=build_info,
            grouped_command_count=grouped_count,
            flat_command_count=flat_count,
            schema_version=schema_version,
            schema_parity_issues=self.community_store.schema_parity_issues(),
            migration_count=max(len(COMMUNITY_SCHEMA_MIGRATIONS), len(migrations)),
            restore_capabilities=restore_capabilities,
            extra_checks={
                'rules_reacceptance_workflow': True,
                'outbound_comment_bridge': True,
                'targeted_digest_workflow': True,
                'migration_plan': migration_plan,
                'runtime_hooks': runtime_hooks,
                'contract_coverage': contract_coverage,
                'snapshot_sections': snapshot_restore_coverage(await self.bot.export_operational_state(guild_id=str(interaction.guild_id or self.settings.discord_guild_id or ''))),
                **surface_policy,
            },
        )
        await interaction.response.send_message(file=discord.File(io.BytesIO(json.dumps(report, ensure_ascii=False, indent=2).encode('utf-8')), filename='nmdiscord-capability-report.json'), ephemeral=True)


    @app_commands.command(name="command_surface_report", description="Показать каноническую схему slash-команд")
    @app_commands.default_permissions(manage_guild=True)
    async def command_surface_report(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        all_commands = self.bot.tree.get_commands()
        grouped_count = len([cmd for cmd in all_commands if isinstance(cmd, app_commands.Group)])
        flat_count = len([cmd for cmd in all_commands if isinstance(cmd, app_commands.Command)])
        payload = command_surface_policy(grouped_count=grouped_count, flat_count=flat_count, mode=self.settings.command_surface_mode, removed_flat_aliases=getattr(self.bot, '_removed_flat_aliases', []), missing_permission_gates=getattr(self.bot, '_missing_permission_gates', []))
        await interaction.response.send_message(file=discord.File(io.BytesIO(json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')), filename='nmdiscord-command-surface.json'), ephemeral=True)

    @app_commands.command(name="layout_legacy_cleanup", description="Удалить legacy-ресурсы, срок удаления которых уже наступил")
    @app_commands.default_permissions(manage_guild=True)
    async def layout_legacy_cleanup(self, interaction: discord.Interaction, limit: app_commands.Range[int,1,50] = 10, apply: bool = False) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        guild = interaction.guild or self.bot.get_guild(self.settings.discord_guild_id)
        if guild is None:
            await interaction.response.send_message('Сервер недоступен.', ephemeral=True)
            return
        rows = await self.community_store.list_legacy_layout_resources(guild_id=str(guild.id), due_only=False, limit=limit)
        due = [row for row in rows if (_parse_datetime(str(row.get('delete_after') or '')) or _utc_now() + timedelta(days=3650)) <= _utc_now()]
        if not apply:
            preview = [f"{row.get('resource_type')}: {row.get('resource_name')}" for row in due[:10]]
            await interaction.response.send_message('К удалению: ' + (', '.join(preview) if preview else 'ничего нет') + '. Для выполнения повторите с apply=true.', ephemeral=True)
            return
        request_id = await self._queue_risky_approval(interaction, kind='legacy_layout_cleanup', payload={'guild_id': str(guild.id), 'limit': int(limit)}, summary='legacy_layout_cleanup')
        await interaction.response.send_message(f'Создан запрос на удаление legacy-ресурсов: №{request_id}.', ephemeral=True)

    @app_commands.command(name="ping", description="Проверить, что бот отвечает")
    async def ping(self, interaction: discord.Interaction) -> None:
        latency_ms = int(self.bot.latency * 1000)
        await interaction.response.send_message(f"Бот отвечает. Задержка шлюза: `{latency_ms} мс`", ephemeral=True)

    @app_commands.command(name="about", description="Информация о проекте NeverMine")
    async def about(self, interaction: discord.Interaction) -> None:
        embed = discord.Embed(
            title=self.settings.nevermine_server_name,
            description=(
                "NeverMine — цифровая вселенная Minecraft с фокусом на RPG, "
                "прогрессию, мировые события и долговременный мир без вайпов."
            ),
            color=EMBED_COLOR,
        )
        embed.add_field(name="Адрес", value=self.settings.nevermine_server_address or "не задан", inline=False)
        embed.set_footer(text="SkiF4er • NeverMine")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="status", description="Статус NeverMine")
    async def status(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        if not self.api.configured():
            await interaction.followup.send(embed=_error_embed("Статус недоступен", "API для статуса ещё не настроен."), ephemeral=True)
            return
        try:
            payload = await self.api.fetch_status()
        except NeverMineApiError as exc:
            await interaction.followup.send(embed=_error_embed("Ошибка запроса статуса", str(exc)), ephemeral=True)
            return
        await interaction.followup.send(embed=build_status_embed(self.settings.nevermine_server_name, payload))

    @app_commands.command(name="players", description="Список игроков онлайн")
    async def players(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        if not self.api.configured():
            await interaction.followup.send(embed=_error_embed("Игроки недоступны", "API списка игроков ещё не настроен."), ephemeral=True)
            return
        try:
            payload = await self.api.fetch_players()
        except NeverMineApiError as exc:
            await interaction.followup.send(embed=_error_embed("Ошибка запроса игроков", str(exc)), ephemeral=True)
            return

        online = _pick(payload, ["online", "players_online", "count"], default=0)
        max_players = _pick(payload, ["max", "players_max", "max_players"], default="?")
        players = payload.get("players") if isinstance(payload.get("players"), list) else []
        player_list = "\n".join(f"- {name}" for name in players[:25]) if players else "Сейчас список пуст или API не отдаёт имена."

        embed = discord.Embed(
            title=f"Игроки онлайн — {self.settings.nevermine_server_name}",
            description=player_list,
            color=EMBED_COLOR,
        )
        embed.add_field(name="Онлайн", value=f"{online}/{max_players}", inline=True)
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="links", description="Полезные ссылки NeverMine")
    async def links(self, interaction: discord.Interaction) -> None:
        embed = discord.Embed(title="Ссылки NeverMine", color=EMBED_COLOR)
        fields = [
            ("Сайт", self.settings.nevermine_website_url),
            ("VK", self.settings.nevermine_vk_url),
            ("Telegram", self.settings.nevermine_telegram_url),
            ("Discord", self.settings.nevermine_discord_invite_url),
        ]
        added = False
        for name, value in fields:
            if value:
                embed.add_field(name=name, value=value, inline=False)
                added = True
        if not added:
            embed.description = "Ссылки пока не заполнены в конфигурации."
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="announce", description="Отправить объявление в настроенный канал")
    @app_commands.default_permissions(manage_messages=True)
    @app_commands.describe(text="Текст объявления")
    async def announce(self, interaction: discord.Interaction, text: str) -> None:
        if not await self._require_scope(interaction, 'community'):
            return
        cooldown = await self.storage.command_cooldown(discord_user_id=interaction.user.id, command_name="announce")
        if cooldown > 0:
            await interaction.response.send_message(f"Подождите {cooldown} сек. перед повторной отправкой объявления.", ephemeral=True)
            return
        if self.settings.approval_required_for_announce and interaction.user.id not in self.settings.admin_user_ids:
            request_id = await self.community_store.create_approval_request(
                kind="announce",
                payload={"text": text, "guild_id": interaction.guild_id},
                requested_by=str(interaction.user.id),
                requested_by_name=str(interaction.user),
                required_role="admin",
            )
            await self.bot.record_audit(action="announce_approval_requested", actor_user_id=interaction.user.id, target_user_id=None, status="pending", payload={"request_id": request_id, "text": text})
            await self.bot.queue_bridge_event("approval.request.created", {"request_id": request_id, "kind": "announce", "requested_by": str(interaction.user.id), "requested_by_name": str(interaction.user)})
            await interaction.response.send_message(f"Объявление отправлено на согласование. ID запроса: {request_id}", ephemeral=True)
            return
        channel = self.bot._get_message_channel(self.settings.discord_announcements_channel_id)
        if channel is None:
            await interaction.response.send_message("DISCORD_ANNOUNCEMENTS_CHANNEL_ID не настроен или канал недоступен.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"{self.settings.nevermine_server_name} — объявление",
            description=text,
            color=STAFF_COLOR,
        )
        embed.set_footer(text=f"Отправил: {interaction.user.display_name}")
        mentions = await self.bot._subscription_event_mentions(interaction.guild, event_kind='community.announcement.created')
        message = await channel.send(content=mentions or None, embed=embed)
        await self.community_store.upsert_external_content_mirror(source_platform='discord', content_kind='announcement', external_content_id=str(message.id), discord_channel_id=str(getattr(channel, 'id', '') or ''), discord_message_id=str(message.id), metadata={'origin': 'discord-command'})
        await self.bot.record_audit(
            action="announce",
            actor_user_id=interaction.user.id,
            target_user_id=None,
            status="success",
            payload={"text": text, "guild_id": interaction.guild_id},
        )
        await self.bot.queue_bridge_event("community.announcement.created", {"text": text, "guild_id": interaction.guild_id, "actor_user_id": str(interaction.user.id), "actor_name": str(interaction.user)})
        LOGGER.info("Объявление отправлено пользователем %s (%s)", interaction.user, interaction.user.id)
        await interaction.response.send_message("Объявление отправлено.", ephemeral=True)

    @app_commands.command(name="announcement_update", description="Обновить существующее объявление")
    @app_commands.default_permissions(manage_messages=True)
    async def announcement_update(self, interaction: discord.Interaction, message_id: str, text: str, title: str | None = None) -> None:
        if not await self._require_scope(interaction, 'community'):
            return
        channel = self.bot._get_message_channel(self.settings.discord_announcements_channel_id)
        if channel is None:
            await interaction.response.send_message('Канал объявлений недоступен.', ephemeral=True)
            return
        message = await _fetch_message_from_channel(channel, message_id)
        if message is None:
            await interaction.response.send_message('Сообщение не найдено.', ephemeral=True)
            return
        embed = message.embeds[0].copy() if message.embeds else discord.Embed(color=STAFF_COLOR)
        embed.title = str(title or embed.title or f"{self.settings.nevermine_server_name} — объявление")[:256]
        embed.description = text[:4000]
        await message.edit(embed=embed)
        await self.community_store.upsert_external_content_mirror(source_platform='discord', content_kind='announcement', external_content_id=str(message.id), discord_channel_id=str(getattr(channel, 'id', '') or ''), discord_message_id=str(message.id), metadata={'origin': 'discord-command', 'updated': True})
        await self.bot.queue_bridge_event('community.announcement.updated', {'message_id': str(message.id), 'external_message_id': str(message.id), 'title': embed.title or '', 'text': text, 'actor_user_id': str(interaction.user.id), 'actor_name': str(interaction.user), 'source_platform': 'discord'})
        await interaction.response.send_message('Объявление обновлено.', ephemeral=True)

    @app_commands.command(name="announcement_delete", description="Удалить объявление и отправить delete lifecycle")
    @app_commands.default_permissions(manage_messages=True)
    async def announcement_delete(self, interaction: discord.Interaction, message_id: str) -> None:
        if not await self._require_scope(interaction, 'community'):
            return
        channel = self.bot._get_message_channel(self.settings.discord_announcements_channel_id)
        if channel is None:
            await interaction.response.send_message('Канал объявлений недоступен.', ephemeral=True)
            return
        message = await _fetch_message_from_channel(channel, message_id)
        if message is None:
            await interaction.response.send_message('Сообщение не найдено.', ephemeral=True)
            return
        await message.delete()
        await self.community_store.delete_external_content_mirror(source_platform='discord', content_kind='announcement', external_content_id=str(message_id))
        await self.bot.queue_bridge_event('community.announcement.deleted', {'message_id': str(message_id), 'external_message_id': str(message_id), 'source_platform': 'discord', 'actor_user_id': str(interaction.user.id)})
        await interaction.response.send_message('Объявление удалено.', ephemeral=True)

    @app_commands.command(name="devlog_publish", description="Опубликовать запись в devlog")
    @app_commands.default_permissions(manage_messages=True)
    async def devlog_publish(self, interaction: discord.Interaction, title: str, text: str, url: str | None = None) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        channel = self.bot._get_message_channel(self.settings.discord_devlog_channel_id)
        if channel is None:
            await interaction.response.send_message('Канал devlog недоступен.', ephemeral=True)
            return
        embed = discord.Embed(title=title[:256], description=text[:4000], color=EMBED_COLOR)
        if url:
            embed.add_field(name='Ссылка', value=url[:1000], inline=False)
        mentions = await self.bot._subscription_event_mentions(interaction.guild, event_kind='community.devlog.created')
        message = await channel.send(content=mentions or None, embed=embed)
        await self.community_store.upsert_external_content_mirror(source_platform='discord', content_kind='devlog', external_content_id=str(message.id), discord_channel_id=str(getattr(channel, 'id', '') or ''), discord_message_id=str(message.id), metadata={'origin': 'discord-command'})
        await self.bot.queue_bridge_event('community.devlog.created', {'message_id': str(message.id), 'external_message_id': str(message.id), 'title': title, 'text': text, 'url': url or '', 'actor_user_id': str(interaction.user.id), 'actor_name': str(interaction.user), 'source_platform': 'discord'})
        await interaction.response.send_message('Запись devlog опубликована.', ephemeral=True)

    @app_commands.command(name="devlog_update", description="Обновить запись devlog")
    @app_commands.default_permissions(manage_messages=True)
    async def devlog_update(self, interaction: discord.Interaction, message_id: str, title: str, text: str, url: str | None = None) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        channel = self.bot._get_message_channel(self.settings.discord_devlog_channel_id)
        if channel is None:
            await interaction.response.send_message('Канал devlog недоступен.', ephemeral=True)
            return
        message = await _fetch_message_from_channel(channel, message_id)
        if message is None:
            await interaction.response.send_message('Сообщение не найдено.', ephemeral=True)
            return
        embed = message.embeds[0].copy() if message.embeds else discord.Embed(color=EMBED_COLOR)
        embed.title = title[:256]
        embed.description = text[:4000]
        if url:
            if embed.fields:
                embed.set_field_at(0, name='Ссылка', value=url[:1000], inline=False)
            else:
                embed.add_field(name='Ссылка', value=url[:1000], inline=False)
        await message.edit(embed=embed)
        await self.community_store.upsert_external_content_mirror(source_platform='discord', content_kind='devlog', external_content_id=str(message.id), discord_channel_id=str(getattr(channel, 'id', '') or ''), discord_message_id=str(message.id), metadata={'origin': 'discord-command', 'updated': True})
        await self.bot.queue_bridge_event('community.devlog.updated', {'message_id': str(message.id), 'external_message_id': str(message.id), 'title': title, 'text': text, 'url': url or '', 'actor_user_id': str(interaction.user.id), 'actor_name': str(interaction.user), 'source_platform': 'discord'})
        await interaction.response.send_message('Запись devlog обновлена.', ephemeral=True)

    @app_commands.command(name="devlog_delete", description="Удалить запись devlog")
    @app_commands.default_permissions(manage_messages=True)
    async def devlog_delete(self, interaction: discord.Interaction, message_id: str) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        channel = self.bot._get_message_channel(self.settings.discord_devlog_channel_id)
        if channel is None:
            await interaction.response.send_message('Канал devlog недоступен.', ephemeral=True)
            return
        message = await _fetch_message_from_channel(channel, message_id)
        if message is None:
            await interaction.response.send_message('Сообщение не найдено.', ephemeral=True)
            return
        await message.delete()
        await self.community_store.delete_external_content_mirror(source_platform='discord', content_kind='devlog', external_content_id=str(message_id))
        await self.bot.queue_bridge_event('community.devlog.deleted', {'message_id': str(message_id), 'external_message_id': str(message_id), 'source_platform': 'discord', 'actor_user_id': str(interaction.user.id)})
        await interaction.response.send_message('Запись devlog удалена.', ephemeral=True)

    @app_commands.command(name="topic_update", description="Обновить заголовок или текст существующей темы")
    @app_commands.default_permissions(manage_messages=True)
    async def topic_update(self, interaction: discord.Interaction, title: str | None = None, body: str | None = None, thread_id: str | None = None) -> None:
        if not await self._require_scope(interaction, 'staff'):
            return
        thread = interaction.channel if isinstance(interaction.channel, discord.Thread) and not thread_id else (self.bot.get_channel(int(thread_id)) if str(thread_id or '').isdigit() else None)
        if not isinstance(thread, discord.Thread):
            await interaction.response.send_message('Укажи thread_id или запусти команду внутри темы.', ephemeral=True)
            return
        topic = await self.community_store.get_forum_topic(str(thread.id))
        if topic is None:
            await interaction.response.send_message('Тема не найдена в registry.', ephemeral=True)
            return
        if title:
            await thread.edit(name=title[:100])
        if body:
            starter = None
            with contextlib.suppress(Exception):
                starter = await thread.fetch_message(thread.id)
            if starter is not None:
                with contextlib.suppress(Exception):
                    await starter.edit(content=body[:1900])
        event_kind = f"{topic_kind_to_event_prefix(str(topic.get('topic_kind') or 'support'))}.updated"
        await self.bot.queue_bridge_event(event_kind, {'thread_id': str(thread.id), 'title': title or str(topic.get('title') or thread.name), 'body': body or '', 'actor_user_id': str(interaction.user.id), 'actor_name': str(interaction.user), 'source_platform': 'discord'})
        await interaction.response.send_message('Тема обновлена.', ephemeral=True)

    @app_commands.command(name="verify_start", description="Начать привязку Discord к учётной записи NeverMine")
    async def verify_start(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True, ephemeral=True)
        if not self.api.verification_configured():
            await interaction.followup.send(embed=_error_embed("Привязка недоступна", "API привязки ещё не настроен."), ephemeral=True)
            return

        cooldown = await self.storage.command_cooldown(discord_user_id=interaction.user.id, command_name="verify_start")
        if cooldown > 0:
            await interaction.followup.send(f"Подождите {cooldown} сек. перед повторным запуском привязки.", ephemeral=True)
            return

        lock_key = f"verify_start:{interaction.user.id}"
        lock_token = await self.storage.acquire_lock(lock_key)
        if self.storage.cache.client is not None and not lock_token:
            await interaction.followup.send("Привязка уже выполняется. Попробуйте ещё раз через несколько секунд.", ephemeral=True)
            return
        try:
            payload = await self.api.start_verification(interaction.user.id, str(interaction.user))
            code = str(payload.get("code", "—"))
            expires_at = str(payload.get("expires_at", ""))
            await self.storage.create_verification_session(
                discord_user_id=interaction.user.id,
                discord_username=str(interaction.user),
                code=code,
                expires_at=expires_at,
                metadata=payload,
            )
            await self.bot.record_audit(
                action="verify_start",
                actor_user_id=interaction.user.id,
                target_user_id=interaction.user.id,
                status="success",
                payload=payload,
            )
        except NeverMineApiError as exc:
            await self.bot.record_audit(
                action="verify_start",
                actor_user_id=interaction.user.id,
                target_user_id=interaction.user.id,
                status="error",
                payload={"error": str(exc)},
            )
            await interaction.followup.send(embed=_error_embed("Ошибка запуска привязки", str(exc)), ephemeral=True)
            return
        finally:
            await self.storage.release_lock(lock_key, lock_token)

        instructions = str(payload.get("instructions", "Введите код в игре или завершите привязку через /verify_complete."))
        embed = discord.Embed(title="Начало привязки", color=EMBED_COLOR)
        embed.add_field(name="Код", value=code, inline=False)
        embed.add_field(name="Истекает", value=expires_at or "—", inline=True)
        embed.add_field(name="Что делать дальше", value=instructions, inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="verify_complete", description="Завершить привязку по коду подтверждения")
    @app_commands.describe(code="Код подтверждения")
    async def verify_complete(self, interaction: discord.Interaction, code: str) -> None:
        await interaction.response.defer(thinking=True, ephemeral=True)
        if not self.api.verification_configured():
            await interaction.followup.send(embed=_error_embed("Привязка недоступна", "API привязки ещё не настроен."), ephemeral=True)
            return

        cooldown = await self.storage.command_cooldown(discord_user_id=interaction.user.id, command_name="verify_complete")
        if cooldown > 0:
            await interaction.followup.send(f"Подождите {cooldown} сек. перед повторным завершением привязки.", ephemeral=True)
            return

        lock_key = f"verify_complete:{interaction.user.id}"
        lock_token = await self.storage.acquire_lock(lock_key)
        if self.storage.cache.client is not None and not lock_token:
            await interaction.followup.send("Завершение привязки уже выполняется. Попробуйте ещё раз через несколько секунд.", ephemeral=True)
            return
        try:
            payload = await self.api.complete_verification(interaction.user.id, code)
            minecraft_username = str(payload.get("minecraft_username") or payload.get("username") or payload.get("player_name") or "—")
            minecraft_uuid = str(payload.get("minecraft_uuid") or payload.get("uuid") or payload.get("player_uuid") or "")

            if minecraft_uuid:
                existing_owner = await self.storage.get_link_by_minecraft_uuid(minecraft_uuid)
                if existing_owner is not None and str(existing_owner.get("discord_user_id")) != str(interaction.user.id):
                    await self.storage.complete_verification_session(
                        discord_user_id=interaction.user.id,
                        code=code,
                        status="conflict",
                        metadata={**payload, "conflict_discord_user_id": existing_owner.get("discord_user_id")},
                    )
                    await self.bot.record_audit(
                        action="verify_complete",
                        actor_user_id=interaction.user.id,
                        target_user_id=interaction.user.id,
                        status="conflict",
                        payload={"minecraft_uuid": minecraft_uuid, "current_owner": existing_owner.get("discord_user_id")},
                    )
                    await interaction.followup.send(
                        embed=_error_embed(
                            "Обнаружен конфликт привязки",
                            "Эта учётная запись Minecraft уже привязана к другому Discord-аккаунту. Обратитесь к staff для безопасной отвязки.",
                        ),
                        ephemeral=True,
                    )
                    return

            await self.storage.complete_verification_session(
                discord_user_id=interaction.user.id,
                code=code,
                status="completed",
                metadata=payload,
            )
            identity = None
            if minecraft_uuid:
                await self.storage.upsert_link(
                    discord_user_id=interaction.user.id,
                    minecraft_username=minecraft_username,
                    minecraft_uuid=minecraft_uuid,
                    metadata=payload,
                )
                await self.community_store.upsert_platform_link(
                    platform="discord",
                    platform_user_id=str(interaction.user.id),
                    platform_username=str(interaction.user),
                    guild_or_chat_id=str(interaction.guild_id) if interaction.guild_id else None,
                    minecraft_username=minecraft_username,
                    minecraft_uuid=minecraft_uuid,
                    metadata=payload,
                )
                await self.community_store.add_platform_link_event(
                    platform="discord",
                    event="linked",
                    platform_user_id=str(interaction.user.id),
                    admin_user_id=None,
                    player_name=minecraft_username,
                    player_uuid=minecraft_uuid,
                    details=payload,
                )
                identity = await self.community_store.get_identity_by_minecraft_uuid(minecraft_uuid)
            await self.bot.record_audit(
                action="verify_complete",
                actor_user_id=interaction.user.id,
                target_user_id=interaction.user.id,
                status="success",
                payload={**payload, "identity": identity or {}},
            )
            await self.bot.queue_bridge_event(
                "identity.discord.linked",
                {
                    "discord_user_id": str(interaction.user.id),
                    "discord_username": str(interaction.user),
                    "minecraft_username": minecraft_username,
                    "minecraft_uuid": minecraft_uuid,
                    "identity": identity or {},
                },
            )
            if interaction.guild is not None:
                member = interaction.guild.get_member(interaction.user.id)
                if member is not None:
                    await self.bot.sync_verified_role(member, linked=True)
        except NeverMineApiError as exc:
            await self.storage.complete_verification_session(
                discord_user_id=interaction.user.id,
                code=code,
                status="error",
                metadata={"error": str(exc)},
            )
            await self.bot.record_audit(
                action="verify_complete",
                actor_user_id=interaction.user.id,
                target_user_id=interaction.user.id,
                status="error",
                payload={"error": str(exc)},
            )
            await interaction.followup.send(embed=_error_embed("Ошибка завершения привязки", str(exc)), ephemeral=True)
            return
        finally:
            await self.storage.release_lock(lock_key, lock_token)

        embed = discord.Embed(title="Привязка завершена", color=EMBED_COLOR)
        embed.add_field(name="Учётная запись Minecraft", value=minecraft_username, inline=True)
        embed.add_field(name="UUID", value=minecraft_uuid or "—", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="verify_status", description="Проверить, привязан ли ваш Discord-аккаунт")
    async def verify_status(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True, ephemeral=True)
        local_link = await self.storage.get_link(interaction.user.id)
        linked = local_link is not None
        username = str(local_link.get("minecraft_username", "—")) if local_link else "—"
        account_uuid = str(local_link.get("minecraft_uuid", "—")) if local_link else "—"
        if self.api.configured():
            try:
                payload = await self.api.fetch_link_status(interaction.user.id)
                linked = bool(payload.get("linked", linked))
                username = str(payload.get("minecraft_username") or payload.get("username") or username)
                account_uuid = str(payload.get("minecraft_uuid") or payload.get("uuid") or account_uuid)
            except NeverMineApiError as exc:
                await interaction.followup.send(embed=_error_embed("Ошибка запроса статуса привязки", str(exc)), ephemeral=True)
                return
        embed = discord.Embed(title="Статус привязки", color=EMBED_COLOR if linked else ERROR_COLOR)
        embed.add_field(name="Состояние", value="Привязано" if linked else "Не привязано", inline=True)
        embed.add_field(name="Учётная запись Minecraft", value=username, inline=True)
        embed.add_field(name="UUID", value=account_uuid, inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="verify_unlink", description="Отвязать Discord-аккаунт от учётной записи NeverMine")
    @app_commands.describe(user_id="ID пользователя Discord для отвязки; пусто = отвязать себя")
    async def verify_unlink(self, interaction: discord.Interaction, user_id: str | None = None) -> None:
        await interaction.response.defer(thinking=True, ephemeral=True)
        target_id = interaction.user.id
        if user_id:
            if not self.bot.is_staff(interaction.user):
                await interaction.followup.send("Недостаточно прав для отвязки другого пользователя.", ephemeral=True)
                return
            try:
                target_id = int(user_id)
            except ValueError:
                await interaction.followup.send("ID пользователя должен быть числом.", ephemeral=True)
                return
        if not self.api.verification_configured():
            await interaction.followup.send(embed=_error_embed("Привязка недоступна", "API привязки ещё не настроен."), ephemeral=True)
            return

        lock_key = f"verify_unlink:{target_id}"
        lock_token = await self.storage.acquire_lock(lock_key)
        if self.storage.cache.client is not None and not lock_token:
            await interaction.followup.send("Отвязка уже выполняется. Попробуйте ещё раз через несколько секунд.", ephemeral=True)
            return
        try:
            if self.settings.approval_required_for_verify_unlink and interaction.user.id not in self.settings.admin_user_ids:
                request_id = await self.community_store.create_approval_request(
                    kind="verify_unlink",
                    payload={"target_id": str(target_id), "guild_id": interaction.guild_id},
                    requested_by=str(interaction.user.id),
                    requested_by_name=str(interaction.user),
                    required_role="admin",
                )
                await self.bot.record_audit(action="verify_unlink_approval_requested", actor_user_id=interaction.user.id, target_user_id=target_id, status="pending", payload={"request_id": request_id})
                await self.bot.queue_bridge_event("approval.request.created", {"request_id": request_id, "kind": "verify_unlink", "target_id": str(target_id), "requested_by": str(interaction.user.id)})
                await interaction.followup.send(f"Запрос на отвязку отправлен на согласование. ID запроса: {request_id}", ephemeral=True)
                return
            payload = await self.api.unlink(target_id)
            await self.storage.unlink(target_id)
            await self.community_store.remove_platform_link(platform="discord", platform_user_id=str(target_id))
            await self.community_store.add_platform_link_event(
                platform="discord",
                event="unlinked",
                platform_user_id=str(target_id),
                admin_user_id=str(interaction.user.id),
                player_name=None,
                player_uuid=None,
                details=payload,
            )
            await self.bot.record_audit(
                action="verify_unlink",
                actor_user_id=interaction.user.id,
                target_user_id=target_id,
                status="success",
                payload=payload,
            )
            await self.bot.queue_bridge_event("identity.discord.unlinked", {"discord_user_id": str(target_id), "actor_user_id": str(interaction.user.id), "payload": payload})
            if interaction.guild is not None:
                member = interaction.guild.get_member(target_id)
                if member is not None:
                    await self.bot.sync_verified_role(member, linked=False)
        except NeverMineApiError as exc:
            await self.bot.record_audit(
                action="verify_unlink",
                actor_user_id=interaction.user.id,
                target_user_id=target_id,
                status="error",
                payload={"error": str(exc)},
            )
            await interaction.followup.send(embed=_error_embed("Ошибка отвязки", str(exc)), ephemeral=True)
            return
        finally:
            await self.storage.release_lock(lock_key, lock_token)
        await interaction.followup.send(str(payload.get("message", "Привязка удалена.")), ephemeral=True)

    @app_commands.command(name="audit_recent", description="Показать последние записи staff/system аудита")
    @app_commands.describe(limit="Количество записей, максимум 20", action="Фильтр по действию")
    async def audit_recent(self, interaction: discord.Interaction, limit: int = 10, action: str | None = None) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        safe_limit = max(1, min(limit, 20))
        rows = await self.storage.list_recent_audit_entries(limit=safe_limit, action=action.strip() if action else None)
        if not rows:
            await interaction.response.send_message("Журнал аудита пока пуст.", ephemeral=True)
            return
        embed = discord.Embed(title="Последние записи аудита", color=AUDIT_COLOR)
        for row in rows:
            payload_preview = _preview_payload(_sanitize_payload(row.get("payload", {})), max_length=180)
            embed.add_field(
                name=f"{row.get('created_at', '—')} • {row.get('action', '—')} • {row.get('status', '—')}",
                value=f"инициатор={row.get('actor_user_id') or '—'} цель={row.get('target_user_id') or '—'}\n{payload_preview}",
                inline=False,
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @app_commands.command(name="audit_search", description="Найти записи аудита по фильтрам")
    @app_commands.default_permissions(manage_guild=True)
    async def audit_search(
        self,
        interaction: discord.Interaction,
        actor_user_id: str | None = None,
        target_user_id: str | None = None,
        status: str | None = None,
        category: str | None = None,
        hours: app_commands.Range[int,1,720] = 24,
        export_format: str = 'embed',
        limit: app_commands.Range[int,1,250] = 20,
    ) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        normalized_format = (export_format or 'embed').strip().lower()
        if normalized_format not in {'embed', 'csv', 'json'}:
            await interaction.response.send_message('Формат должен быть одним из: embed, csv, json.', ephemeral=True)
            return
        rows = await self.storage.search_audit_entries(
            actor_user_id=actor_user_id,
            target_user_id=target_user_id,
            status=status,
            category=category,
            hours=hours,
            limit=limit,
        )
        if not rows:
            await interaction.response.send_message('По заданным фильтрам записи аудита не найдены.', ephemeral=True)
            return
        if normalized_format == 'embed':
            embed = self._ops_embed('Аудит / поиск')
            embed.add_field(name='Найдено', value=str(len(rows)), inline=True)
            embed.add_field(name='Окно', value=f'{hours} ч.', inline=True)
            for row in rows[:10]:
                payload_preview = _preview_payload(_sanitize_payload(row.get('payload', {})), max_length=140)
                embed.add_field(name=f"{row.get('created_at','—')} • {row.get('action','—')}", value=f"статус={row.get('status') or '—'} • инициатор={row.get('actor_user_id') or '—'} • цель={row.get('target_user_id') or '—'}\n{payload_preview}", inline=False)
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        if normalized_format == 'csv':
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=['created_at','action','actor_user_id','target_user_id','status','payload'])
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    'created_at': row.get('created_at') or '',
                    'action': row.get('action') or '',
                    'actor_user_id': row.get('actor_user_id') or '',
                    'target_user_id': row.get('target_user_id') or '',
                    'status': row.get('status') or '',
                    'payload': json.dumps(_sanitize_payload(row.get('payload', {})), ensure_ascii=False, sort_keys=True),
                })
            data = buf.getvalue().encode('utf-8')
            await interaction.response.send_message(
                f'Экспортировано записей аудита: {len(rows)}.',
                file=discord.File(io.BytesIO(data), filename='nmdiscord-audit-search.csv'),
                ephemeral=True,
            )
            return
        data = json.dumps({'hours': hours, 'count': len(rows), 'rows': rows}, ensure_ascii=False, indent=2, default=str).encode('utf-8')
        await interaction.response.send_message(
            f'Экспортировано записей аудита: {len(rows)}.',
            file=discord.File(io.BytesIO(data), filename='nmdiscord-audit-search.json'),
            ephemeral=True,
        )

    @app_commands.command(name="approval_recent", description="Показать последние запросы на согласование")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(limit="Количество записей, максимум 20", status="Фильтр: ожидает/одобрено/отклонено")
    async def approval_recent(self, interaction: discord.Interaction, limit: int = 10, status: str | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        normalized_status = normalize_approval_status(status) if status else None
        rows = await self.community_store.list_approval_requests(status=normalized_status, limit=max(1, min(limit, 20)))
        if not rows:
            await interaction.response.send_message("Очередь согласований пока пуста.", ephemeral=True)
            return
        embed = discord.Embed(title="Запросы на согласование", color=AUDIT_COLOR)
        for row in rows:
            payload_preview = _preview_payload(_sanitize_payload(row.get("payload_json", {})), max_length=180)
            approvals = row.get('approvals_json') or []
            expires_at = str(row.get('expires_at') or '—')
            embed.add_field(
                name=f"№{row.get('id')} • {row.get('kind')} • { {'pending':'ожидает','approved':'одобрено','rejected':'отклонено'}.get(str(row.get('status')), str(row.get('status'))) }",
                value=f"запросил={row.get('requested_by')} обработал={row.get('acted_by') or '—'} • quorum={row.get('required_approvals') or 1} • голосов={len(approvals)} • expires_at={expires_at}\n{payload_preview}",
                inline=False,
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="approval_decide", description="Одобрить или отклонить запрос на согласование")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(request_id=_autocomplete_request_id)
    @app_commands.describe(request_id="ID запроса на согласование", decision="одобрить/отклонить", note="Комментарий")
    async def approval_decide(self, interaction: discord.Interaction, request_id: int, decision: str, note: str | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        normalized = normalize_approval_decision(decision)
        if normalized is None:
            await interaction.response.send_message("Решение должно быть одним из: одобрить или отклонить.", ephemeral=True)
            return
        result = await self.community_store.decide_approval_request(request_id, decision=normalized, acted_by=str(interaction.user.id), note=note or '', rejection_reason_code='manual_rejection' if normalized == 'rejected' else '')
        if result is None:
            await interaction.response.send_message("Запрос на согласование не найден.", ephemeral=True)
            return
        if not result.get('ok'):
            reason = str(result.get('reason') or 'not_pending')
            message = 'Запрос уже обработан.' if reason == 'not_pending' else ('Срок действия запроса истёк.' if reason == 'expired' else 'Не удалось обработать запрос.')
            await interaction.response.send_message(message, ephemeral=True)
            return
        new_status = str(result.get('status') or normalized)
        rows = await self.community_store.list_approval_requests(limit=20)
        row = next((item for item in rows if int(item.get("id", 0)) == request_id), None)
        if result.get('final') and new_status == "approved" and row is not None:
            payload = row.get("payload_json") or {}
            kind = str(row.get("kind") or "")
            if kind == "announce":
                channel = self.bot._get_message_channel(self.settings.discord_announcements_channel_id)
                if channel is not None:
                    embed = discord.Embed(title=f"{self.settings.nevermine_server_name} — объявление", description=str(payload.get("text") or "—"), color=STAFF_COLOR)
                    embed.set_footer(text=f"Одобрил: {interaction.user.display_name}")
                    mention = self._interest_ping_mentions('community.announcement.created')
                    await channel.send(content=mention or None, embed=embed)
                    await self.bot.queue_bridge_event("community.announcement.created", {"text": str(payload.get("text") or ""), "approved_by": str(interaction.user.id), "source": "discord-approval"})
            elif kind in {'layout_repair', 'state_restore', 'bridge_dead_letter_requeue'}:
                result_note = await self._execute_approval_payload(kind, payload, approver_user_id=interaction.user.id)
                note = (note or '').strip()
                note = (note + ' | ' if note else '') + result_note
        await self.bot.record_audit(action="approval_decide", actor_user_id=interaction.user.id, target_user_id=None, status="success", payload={"request_id": request_id, "decision": new_status, "note": note or ""})
        if not result.get('final'):
            await interaction.response.send_message(f"Голос сохранён. Для финального одобрения запроса №{request_id} нужно ещё {int(result.get('waiting_for') or 0)} подтверждение(я).", ephemeral=True)
            return
        result_label = "одобрено" if new_status == "approved" else "отклонено"
        await interaction.response.send_message(f"Запрос на согласование №{request_id}: {result_label}", ephemeral=True)

    @app_commands.command(name="panel_publish", description="Опубликовать или обновить системную Discord-панель")
    @app_commands.default_permissions(manage_messages=True)
    @app_commands.describe(panel="Тип панели")
    @app_commands.choices(panel=[
        app_commands.Choice(name="панель входа", value="onboarding"),
        app_commands.Choice(name="панель ролей интересов", value="interest_roles"),
        app_commands.Choice(name="панель навигации", value="help"),
    ])
    async def panel_publish(self, interaction: discord.Interaction, panel: app_commands.Choice[str]) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        if interaction.guild_id is None:
            await interaction.response.send_message("Команда доступна только внутри сервера.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        message = await self.bot.publish_panel(guild_id=interaction.guild_id, panel_type=panel.value, actor_user_id=interaction.user.id)
        await interaction.followup.send(f"{panel_type_label(panel.value).capitalize()} опубликована или обновлена: {message.jump_url}", ephemeral=True)

    @app_commands.command(name="help_nav", description="Показать навигацию по Discord-серверу NeverMine")
    @app_commands.describe(topic="Тема справки: старт/правила/роли/вопросы/поддержка/баги/предложения/гильдии/события/апелляции")
    async def help_nav(self, interaction: discord.Interaction, topic: str = "старт") -> None:
        await interaction.response.send_message(embed=self._help_topic_embed(topic), ephemeral=True)

    @app_commands.command(name="faq", description="Быстрый FAQ по Discord-серверу NeverMine")
    @app_commands.describe(topic="старт/правила/роли/вопросы/поддержка/баги/предложения/гильдии/события/апелляции")
    async def faq(self, interaction: discord.Interaction, topic: str = "вопросы") -> None:
        await interaction.response.send_message(embed=self._help_topic_embed(topic), ephemeral=True)

    @app_commands.command(name="support_topic", description="Создать тему поддержки в нужном форуме")
    @app_commands.describe(area="общая/лаунчер/аккаунт/апелляция", title="Заголовок", details="Описание проблемы", attachment="Вложение: скриншот, лог или файл")
    async def support_topic(self, interaction: discord.Interaction, area: str, title: str, details: str, attachment: discord.Attachment | None = None, attachment_2: discord.Attachment | None = None, attachment_3: discord.Attachment | None = None) -> None:
        if not await self._enforce_command_cooldown(interaction, 'support_topic'):
            return
        area_map = {
            'general': self.settings.discord_forum_help_channel_id,
            'launcher': self.settings.discord_forum_launcher_and_tech_channel_id,
            'account': self.settings.discord_forum_account_help_channel_id,
            'appeal': self.settings.discord_forum_appeals_channel_id,
        }
        key = normalize_support_area(area)
        forum_id = area_map.get(key or '')
        if forum_id is None:
            await interaction.response.send_message("Зона должна быть одной из: общая, лаунчер, аккаунт или апелляция.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        thread, error = await self._create_forum_topic(
            interaction=interaction,
            forum_channel_id=forum_id,
            title=title,
            body=details,
            audit_action=f"support_topic_{key}",
            bridge_event_kind="community.support.created",
            topic_kind='support',
            extra_payload={"area": key},
            attachments=self._iter_attachments(attachment, attachment_2, attachment_3),
        )
        if error:
            await interaction.followup.send(error, ephemeral=True)
            return
        await interaction.followup.send(f"Тема поддержки создана: {thread.mention if thread else 'готово'}", ephemeral=True)

    @app_commands.command(name="bug_report", description="Создать тему о баге")
    @app_commands.describe(attachment="Вложение: скриншот, лог или файл")
    async def bug_report(self, interaction: discord.Interaction, title: str, details: str, attachment: discord.Attachment | None = None, attachment_2: discord.Attachment | None = None, attachment_3: discord.Attachment | None = None) -> None:
        if not await self._enforce_command_cooldown(interaction, 'bug_report'):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        thread, error = await self._create_forum_topic(
            interaction=interaction,
            forum_channel_id=self.settings.discord_forum_bug_reports_channel_id,
            title=title,
            body=details,
            audit_action="bug_report_created",
            bridge_event_kind="community.bug_report.created",
            topic_kind='bug',
            attachments=self._iter_attachments(attachment, attachment_2, attachment_3),
        )
        if error:
            await interaction.followup.send(error, ephemeral=True)
            return
        await interaction.followup.send(f"Баг-репорт создан: {thread.mention if thread else 'готово'}", ephemeral=True)

    @app_commands.command(name="suggestion", description="Создать тему с предложением")
    @app_commands.describe(attachment="Вложение: макет, скриншот или файл")
    async def suggestion(self, interaction: discord.Interaction, title: str, details: str, attachment: discord.Attachment | None = None, attachment_2: discord.Attachment | None = None, attachment_3: discord.Attachment | None = None) -> None:
        if not await self._enforce_command_cooldown(interaction, 'suggestion'):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        thread, error = await self._create_forum_topic(
            interaction=interaction,
            forum_channel_id=self.settings.discord_forum_suggestions_channel_id,
            title=title,
            body=details,
            audit_action="suggestion_created",
            bridge_event_kind="community.suggestion.created",
            topic_kind='suggestion',
            attachments=self._iter_attachments(attachment, attachment_2, attachment_3),
        )
        if error:
            await interaction.followup.send(error, ephemeral=True)
            return
        await interaction.followup.send(f"Предложение создано: {thread.mention if thread else 'готово'}", ephemeral=True)

    @app_commands.command(name="report", description="Отправить жалобу/репорт staff-команде")
    @app_commands.describe(attachment="Вложение: скриншот, лог или файл")
    async def report(self, interaction: discord.Interaction, member: discord.Member, details: str, attachment: discord.Attachment | None = None, attachment_2: discord.Attachment | None = None, attachment_3: discord.Attachment | None = None) -> None:
        if not await self._enforce_command_cooldown(interaction, 'report'):
            return
        if self.settings.discord_reports_channel_id is None:
            await interaction.response.send_message("Канал с жалобами #reports не настроен.", ephemeral=True)
            return
        channel = self.bot._get_message_channel(self.settings.discord_reports_channel_id)
        if channel is None:
            await interaction.response.send_message("Канал с жалобами #reports недоступен.", ephemeral=True)
            return
        duplicate = await self.community_store.find_duplicate_forum_topic(guild_id=str(interaction.guild_id or ''), topic_kind='report', owner_user_id=str(interaction.user.id), title=f"Жалоба на {member.display_name}", target_user_id=str(member.id))
        if duplicate is not None:
            await interaction.response.send_message(f"Похожая активная жалоба уже существует: <#{duplicate.get('thread_id')}>", ephemeral=True)
            return
        embed = discord.Embed(title="Новая жалоба", color=ERROR_COLOR)
        embed.add_field(name="Отправитель", value=interaction.user.mention, inline=False)
        embed.add_field(name="Участник", value=member.mention, inline=False)
        attachments = self._iter_attachments(attachment, attachment_2, attachment_3)
        error = self._validate_attachments('report', attachments)
        if error:
            await interaction.response.send_message(error, ephemeral=True)
            return
        attach_text, attach_meta = self._attachments_section(attachments)
        embed.description = (details[:3000] + attach_text)[:4000]
        staff_owner = None
        if interaction.guild:
            for candidate in interaction.guild.members:
                if self.bot.is_staff(candidate):
                    staff_owner = candidate
                    break
        if staff_owner is not None:
            embed.add_field(name="Ответственный по умолчанию", value=staff_owner.mention, inline=False)
        message = await channel.send(embed=embed)
        thread = None
        with contextlib.suppress(Exception):
            thread = await message.create_thread(name=f"report-{member.display_name}"[:100], auto_archive_duration=1440)
        topic_ref = str(getattr(thread, 'id', '') or getattr(message, 'id', '') or '')
        metadata = {'target_user_id': str(member.id), 'details_preview': details[:256], 'staff_owner_user_id': str(staff_owner.id) if staff_owner else '', 'report_root_message_id': str(getattr(message, 'id', '') or ''), **attach_meta}
        await self.community_store.register_forum_topic(thread_id=topic_ref, guild_id=str(interaction.guild_id or ''), forum_channel_id=str(self.settings.discord_reports_channel_id), topic_kind='report', owner_user_id=str(interaction.user.id), title=f"Жалоба на {member.display_name}", tags=[self.settings.forum_tag_status_open_name], metadata=metadata, auto_close_after_seconds=self.settings.report_escalation_hours * 3600)
        if staff_owner is not None:
            await self.community_store.assign_forum_topic_owner(thread_id=topic_ref, staff_user_id=str(staff_owner.id), staff_name=str(staff_owner))
            if thread is not None:
                await thread.send(f'<@{staff_owner.id}> назначен(а) ответственным за новую жалобу.')
            else:
                await channel.send(f'<@{staff_owner.id}> назначен(а) ответственным за новую жалобу #{message.id}.')
        await self.community_store.upsert_external_discussion_mirror(source_platform='discord', external_topic_id=topic_ref, topic_kind='report', discord_object_id=topic_ref, channel_id=str(getattr(channel, 'id', '') or ''), metadata={'origin': 'discord-command'})
        await self.bot.record_audit(action="report_created", actor_user_id=interaction.user.id, target_user_id=member.id, status="success", payload={"details": details, "reports_channel_id": self.settings.discord_reports_channel_id, 'message_id': getattr(message,'id',None), 'thread_id': topic_ref})
        await self.bot.queue_bridge_event("community.report.created", {"thread_id": topic_ref, "reporter_id": str(interaction.user.id), "target_id": str(member.id), "details": details, "guild_id": str(interaction.guild_id or ''), 'staff_owner_user_id': str(staff_owner.id) if staff_owner else '', **attach_meta})
        await interaction.response.send_message(f"Жалоба отправлена staff-команде: {thread.mention if thread else 'готово'}", ephemeral=True)

    @app_commands.command(name="appeal", description="Создать апелляцию")
    @app_commands.describe(attachment="Вложение: скриншот, лог или файл")
    async def appeal(self, interaction: discord.Interaction, title: str, details: str, attachment: discord.Attachment | None = None, attachment_2: discord.Attachment | None = None, attachment_3: discord.Attachment | None = None) -> None:
        if not await self._enforce_command_cooldown(interaction, 'appeal'):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        thread, error = await self._create_forum_topic(
            interaction=interaction,
            forum_channel_id=self.settings.discord_forum_appeals_channel_id,
            title=title,
            body=details,
            audit_action="appeal_created",
            bridge_event_kind="community.appeal.created",
            topic_kind='appeal',
            attachments=self._iter_attachments(attachment, attachment_2, attachment_3),
        )
        if error:
            await interaction.followup.send(error, ephemeral=True)
            return
        await interaction.followup.send(f"Апелляция создана: {thread.mention if thread else 'готово'}", ephemeral=True)

    @app_commands.command(name="stage_announce", description="Отправить анонс stage/session в Discord")
    @app_commands.default_permissions(manage_events=True)
    async def stage_announce(self, interaction: discord.Interaction, title: str, description: str, starts_at: str | None = None) -> None:
        if not await self._require_scope(interaction, 'events'):
            return
        dt = _parse_datetime(starts_at)
        if dt is not None and dt > _utc_now():
            payload = {"title": title, "description": description, "starts_at": _format_dt(dt)}
            job_id = await self.community_store.schedule_job(job_type='stage_announce', run_at=_format_dt(dt), payload=payload, guild_id=str(interaction.guild_id or ''), channel_id=str(self.settings.discord_announcements_channel_id or self.settings.discord_events_channel_id or ''), created_by=str(interaction.user.id))
            await self.bot.record_audit(action="stage_announce_scheduled", actor_user_id=interaction.user.id, target_user_id=None, status="success", payload={"job_id": job_id, **payload})
            await interaction.response.send_message(f"Анонс сцены запланирован на {dt.isoformat()} (задача №{job_id}).", ephemeral=True)
            return
        sent = await self.bot._send_stage_announcement(title=title, description=description, starts_at=starts_at or '', actor_user_id=interaction.user.id)
        await interaction.response.send_message(f"Анонс сцены отправлен в {sent} канал(а/ов).", ephemeral=True)

    @app_commands.command(name="event_reminder", description="Отправить напоминание о событии")
    @app_commands.default_permissions(manage_events=True)
    async def event_reminder(self, interaction: discord.Interaction, title: str, description: str, starts_at: str | None = None) -> None:
        if not await self._require_scope(interaction, 'events'):
            return
        dt = _parse_datetime(starts_at)
        if dt is not None and dt > _utc_now():
            payload = {"title": title, "description": description, "starts_at": _format_dt(dt)}
            job_id = await self.community_store.schedule_job(job_type='event_reminder', run_at=_format_dt(dt), payload=payload, guild_id=str(interaction.guild_id or ''), channel_id=str(self.settings.discord_events_channel_id or ''), created_by=str(interaction.user.id))
            await self.bot.record_audit(action="event_reminder_scheduled", actor_user_id=interaction.user.id, target_user_id=None, status="success", payload={"job_id": job_id, **payload})
            await interaction.response.send_message(f"Напоминание запланировано на {dt.isoformat()} (задача №{job_id}).", ephemeral=True)
            return
        await self.bot._send_event_reminder(title=title, description=description, starts_at=starts_at or '', actor_user_id=interaction.user.id)
        await interaction.response.send_message("Напоминание отправлено.", ephemeral=True)

    @app_commands.command(name="guild_recruit", description="Создать тему набора в гильдию")
    @app_commands.describe(attachment="Вложение: баннер, скриншот или файл")
    async def guild_recruit(self, interaction: discord.Interaction, title: str, description: str, requirements: str | None = None, attachment: discord.Attachment | None = None, attachment_2: discord.Attachment | None = None, attachment_3: discord.Attachment | None = None) -> None:
        allowed = self.bot.is_staff(interaction.user) or (isinstance(interaction.user, discord.Member) and self.settings.guild_leader_role_id and any(role.id == self.settings.guild_leader_role_id for role in interaction.user.roles))
        if not allowed:
            await interaction.response.send_message("Нужна роль руководителя гильдии или staff-права.", ephemeral=True)
            return
        if not await self._enforce_command_cooldown(interaction, 'guild_recruit', seconds=self.settings.guild_recruit_command_cooldown_seconds):
            return
        body = description.strip()
        if requirements:
            body += f"\n\nТребования:\n{requirements.strip()}"
        await interaction.response.defer(ephemeral=True, thinking=True)
        thread, error = await self._create_forum_topic(
            interaction=interaction,
            forum_channel_id=self.settings.discord_forum_guild_recruitment_channel_id,
            title=title,
            body=body,
            audit_action="guild_recruitment_created",
            bridge_event_kind="community.guild_recruitment.created",
            topic_kind='guild_recruitment',
            extra_payload={"guild_leader_id": str(interaction.user.id), "requirements": requirements or ""},
            attachments=self._iter_attachments(attachment, attachment_2, attachment_3),
        )
        if error:
            await interaction.followup.send(error, ephemeral=True)
            return
        await interaction.followup.send(f"Набор в гильдию опубликован: {thread.mention if thread else 'готово'}", ephemeral=True)

    @app_commands.command(name="guild_recruit_close", description="Закрыть тему набора в гильдию")
    async def guild_recruit_close(self, interaction: discord.Interaction, thread_id: str | None = None, reason: str | None = None) -> None:
        allowed = self.bot.is_staff(interaction.user) or (isinstance(interaction.user, discord.Member) and self.settings.guild_leader_role_id and any(role.id == self.settings.guild_leader_role_id for role in interaction.user.roles))
        if not allowed:
            await interaction.response.send_message("Нужна роль руководителя гильдии или staff-права.", ephemeral=True)
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        channel = self.bot.get_channel(int(resolved_thread_id)) if resolved_thread_id and resolved_thread_id.isdigit() else None
        if not isinstance(channel, discord.Thread):
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        record = await self.community_store.get_forum_topic(str(channel.id)) or {}
        metadata = dict(record.get('metadata_json') or {})
        metadata['close_reason'] = reason or ''
        await self.bot._sync_topic_presentation(channel, topic_kind='guild_recruitment', status='closed', metadata=metadata, archive_override=True)
        await self.bot.record_audit(action="guild_recruitment_closed", actor_user_id=interaction.user.id, target_user_id=None, status="success", payload={"thread_id": thread_id, "reason": reason or ""})
        await self.bot.queue_bridge_event("community.guild_recruitment.closed", {"thread_id": thread_id, "reason": reason or "", "actor_user_id": str(interaction.user.id)})
        await interaction.response.send_message("Тема набора закрыта.", ephemeral=True)


    @app_commands.command(name="guild_recruit_reopen", description="Переоткрыть тему набора в гильдию")
    async def guild_recruit_reopen(self, interaction: discord.Interaction, thread_id: str | None = None) -> None:
        allowed = self.bot.is_staff(interaction.user) or (isinstance(interaction.user, discord.Member) and self.settings.guild_leader_role_id and any(role.id == self.settings.guild_leader_role_id for role in interaction.user.roles))
        if not allowed:
            await interaction.response.send_message("Нужна роль руководителя гильдии или staff-права.", ephemeral=True)
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        thread = await self.bot._resolve_thread(resolved_thread_id or '')
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        record = await self.community_store.get_forum_topic(str(thread.id)) or {}
        metadata = dict(record.get('metadata_json') or {})
        metadata['reopened_at'] = _format_dt(_utc_now())
        await self.bot._sync_topic_presentation(thread, topic_kind='guild_recruitment', status='open', metadata=metadata, archive_override=False)
        await interaction.response.send_message("Тема набора снова открыта.", ephemeral=True)

    @app_commands.command(name="guild_recruit_pause", description="Поставить тему набора в гильдию на паузу")
    async def guild_recruit_pause(self, interaction: discord.Interaction, thread_id: str | None = None, note: str | None = None) -> None:
        allowed = self.bot.is_staff(interaction.user) or (isinstance(interaction.user, discord.Member) and self.settings.guild_leader_role_id and any(role.id == self.settings.guild_leader_role_id for role in interaction.user.roles))
        if not allowed:
            await interaction.response.send_message("Нужна роль руководителя гильдии или staff-права.", ephemeral=True)
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        thread = await self.bot._resolve_thread(resolved_thread_id or '')
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        record = await self.community_store.get_forum_topic(str(thread.id)) or {}
        metadata = dict(record.get('metadata_json') or {})
        metadata['pause_note'] = note or ''
        await self.bot._sync_topic_presentation(thread, topic_kind='guild_recruitment', status='in_review', metadata=metadata, archive_override=False)
        await interaction.response.send_message("Тема набора переведена в режим паузы/рассмотрения.", ephemeral=True)

    @app_commands.command(name="guild_recruit_bump", description="Поднять тему набора в гильдию и продлить активность")
    async def guild_recruit_bump(self, interaction: discord.Interaction, thread_id: str | None = None, note: str | None = None) -> None:
        allowed = self.bot.is_staff(interaction.user) or (isinstance(interaction.user, discord.Member) and self.settings.guild_leader_role_id and any(role.id == self.settings.guild_leader_role_id for role in interaction.user.roles))
        if not allowed:
            await interaction.response.send_message("Нужна роль руководителя гильдии или staff-права.", ephemeral=True)
            return
        if not await self._enforce_command_cooldown(interaction, 'guild_recruit_bump', seconds=self.settings.guild_recruit_command_cooldown_seconds):
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        thread = await self.bot._resolve_thread(resolved_thread_id or '')
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        record = await self.community_store.get_forum_topic(str(thread.id)) or {}
        metadata = dict(record.get('metadata_json') or {})
        metadata['bumped_at'] = _format_dt(_utc_now())
        metadata['bump_note'] = note or ''
        await self.bot._sync_topic_presentation(thread, topic_kind='guild_recruitment', status='open', metadata=metadata, archive_override=False)
        await thread.send(f"Тема набора поднята {'— ' + note if note else 'и снова помечена как активная.'}")
        await interaction.response.send_message("Тема набора поднята и обновлена.", ephemeral=True)

    @app_commands.command(name="topic_claim", description="Назначить себя ответственным за тему поддержки/апелляции/жалобы")
    async def topic_claim(self, interaction: discord.Interaction, thread_id: str | None = None) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        thread = await self.bot._resolve_thread(resolved_thread_id or '')
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        await self.community_store.assign_forum_topic_owner(thread_id=str(thread.id), staff_user_id=str(interaction.user.id), staff_name=str(interaction.user))
        await self.community_store.note_forum_staff_response(thread_id=str(thread.id), staff_user_id=str(interaction.user.id), staff_name=str(interaction.user))
        await self._notify_topic_owner(thread=thread, owner_user_id=str(interaction.user.id), text="ты назначен(а) ответственным за эту тему.")
        await interaction.response.send_message("Ты назначен ответственным за тему.", ephemeral=True)

    @app_commands.command(name="identity_card", description="Показать карточку связей пользователя на разных платформах")
    async def identity_card(self, interaction: discord.Interaction, member: discord.Member | None = None) -> None:
        target = member or (interaction.user if isinstance(interaction.user, discord.Member) else None)
        if target is None:
            await interaction.response.send_message("Не удалось определить пользователя.", ephemeral=True)
            return
        if target.id != interaction.user.id and not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Просмотр чужой карточки связей доступен только staff.", ephemeral=True)
            return
        link = await self.storage.get_link(target.id)
        identity = None
        if link and link.get('minecraft_uuid'):
            identity = await self.community_store.get_identity_by_minecraft_uuid(str(link.get('minecraft_uuid')))
        if identity is None:
            identity = await self.community_store.get_identity_by_discord_user_id(str(target.id))
        events = await self.community_store.list_recent_platform_link_events(player_uuid=str((link or {}).get('minecraft_uuid') or ''), platform_user_id=str(target.id), limit=5)
        embed = discord.Embed(title=f"Карточка связей — {target.display_name}", color=EMBED_COLOR)
        embed.add_field(name="Discord", value=f"{target.mention}\nID: `{target.id}`", inline=False)
        if link:
            embed.add_field(name="Minecraft / NeverMine", value=f"{link.get('minecraft_username') or '—'}\nUUID: `{link.get('minecraft_uuid') or '—'}`", inline=False)
        if identity:
            embed.add_field(name="Telegram", value=f"ID пользователя: `{identity.get('telegram_user_id') or '—'}`\nИмя пользователя: `{identity.get('telegram_username') or '—'}`", inline=False)
            embed.add_field(name="VK", value=f"ID пользователя: `{identity.get('vk_user_id') or '—'}`", inline=False)
            embed.add_field(name="Workspace", value=f"ID участника: `{identity.get('workspace_actor_id') or '—'}`", inline=False)
        if events:
            preview = '\n'.join(f"- {row.get('created_at', '—')} • {row.get('platform', '—')} • {row.get('event', '—')}" for row in events[:5])
            embed.add_field(name="Последние события привязки", value=preview, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @app_commands.command(name="policy_snapshot", description="Показать machine-readable snapshot активной runtime-политики")
    @app_commands.default_permissions(manage_guild=True)
    async def policy_snapshot(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        payload = {
            'forum_policy_overrides': getattr(self.bot, 'runtime_forum_policy_overrides', {}),
            'bridge_event_rules': {k:list(v) for k,v in self.settings.bridge_event_rules.items()},
            'bridge_payload_allowlist': {k:list(v) for k,v in self.settings.bridge_payload_allowlist.items()},
            'attachment_policy': self.settings.forum_attachment_policy,
            'maintenance_mode': await self.bot.get_maintenance_mode(),
            'staff_scope_role_map': {k:list(v) for k,v in self.settings.staff_scope_role_map.items()},
        }
        raw = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode('utf-8')
        await interaction.response.send_message('Snapshot активной runtime-политики сформирован.', file=discord.File(io.BytesIO(raw), filename='nmdiscordbot-policy-snapshot.json'), ephemeral=True)

    @app_commands.command(name="bridge_policy", description="Показать текущую политику синхронизации между платформами")
    @app_commands.default_permissions(manage_guild=True)
    async def bridge_policy(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        embed = discord.Embed(title="Политика синхронизации", color=AUDIT_COLOR)
        embed.add_field(name="Объявления", value=bool_label(self.settings.bridge_sync_announcements), inline=True)
        embed.add_field(name="События", value=bool_label(self.settings.bridge_sync_events), inline=True)
        embed.add_field(name="Поддержка", value=bool_label(self.settings.bridge_sync_support), inline=True)
        embed.add_field(name="Жалобы", value=bool_label(self.settings.bridge_sync_reports), inline=True)
        embed.add_field(name="Набор в гильдию", value=bool_label(self.settings.bridge_sync_guild_recruitment), inline=True)
        embed.add_field(name="Связи аккаунтов", value=bool_label(self.settings.bridge_sync_identity), inline=True)
        destinations = [self.bot._bridge_destination_label(dest) for dest in self.bot._bridge_destinations()]
        embed.add_field(name="Назначения", value='\n'.join(destinations) if destinations else 'Нет', inline=False)
        if self.settings.bridge_payload_allowlist:
            preview = []
            for event_kind, fields in list(self.settings.bridge_payload_allowlist.items())[:5]:
                preview.append(f"- {event_kind}: {', '.join(fields) if fields else 'без ограничений'}")
            embed.add_field(name="Правила состава payload", value='\n'.join(preview), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @app_commands.command(name="bridge_preview", description="Показать маршрутизацию одного bridge-события")
    @app_commands.default_permissions(manage_guild=True)
    async def bridge_preview(self, interaction: discord.Interaction, event_kind: str = "community.announcement.created") -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        from .bridge_doctor import build_bridge_route_preview

        preview = build_bridge_route_preview(self.settings, event_kind)
        embed = discord.Embed(
            title="Bridge route preview",
            description=f"`{preview.get('event_kind')}`",
            color=AUDIT_COLOR if preview.get("ok") else ERROR_COLOR,
        )
        embed.add_field(name="Routing mode", value=str(preview.get("routing_mode") or "—"), inline=True)
        embed.add_field(name="Payload validator", value=bool_label(bool(preview.get("payload_validator_exists"))), inline=True)
        embed.add_field(name="Outbound auth", value="HMAC" if preview.get("outbound_auth", {}).get("hmac_configured") else ("Bearer" if preview.get("outbound_auth", {}).get("bearer_configured") else "нет"), inline=True)
        resolved = list(preview.get("resolved_destinations") or [])
        configured = list(preview.get("configured_destinations") or [])
        missing = list(preview.get("missing_destination_urls") or [])
        unsupported = list(preview.get("unsupported_targets") or [])
        embed.add_field(name="Routes", value=", ".join(resolved) if resolved else "нет", inline=False)
        embed.add_field(name="Configured destinations", value=", ".join(configured) if configured else "нет", inline=False)
        if missing:
            embed.add_field(name="Missing destination URLs", value=", ".join(missing), inline=False)
        if unsupported:
            embed.add_field(name="Unsupported targets", value=", ".join(unsupported), inline=False)
        raw = json.dumps(preview, ensure_ascii=False, indent=2).encode("utf-8")
        await interaction.response.send_message(
            embed=embed,
            file=discord.File(io.BytesIO(raw), filename=f"bridge-preview-{str(event_kind).replace('.', '-')}.json"),
            ephemeral=True,
        )

    @app_commands.command(name="event_coverage", description="Показать покрытие event-contract и bridge-routes")
    @app_commands.default_permissions(manage_guild=True)
    async def event_coverage(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        from .event_coverage import build_event_coverage_report, render_event_coverage_summary

        report = build_event_coverage_report(self.settings)
        totals = report.get("totals") or {}
        embed = discord.Embed(
            title="Event contract coverage",
            description=render_event_coverage_summary(report),
            color=AUDIT_COLOR if report.get("ok") else ERROR_COLOR,
        )
        embed.add_field(name="Declared", value=str(totals.get("declared_event_types", 0)), inline=True)
        embed.add_field(name="Routed", value=str(totals.get("routed_event_types", 0)), inline=True)
        embed.add_field(name="Unrouted", value=str(totals.get("unrouted_event_types", 0)), inline=True)
        destination_counts = report.get("destination_counts") or {}
        if destination_counts:
            embed.add_field(
                name="Destinations",
                value="\n".join(f"{name}: {count}" for name, count in sorted(destination_counts.items())) or "нет",
                inline=False,
            )
        unknown = list(report.get("unknown_rule_keys") or [])
        if unknown:
            embed.add_field(name="Unknown rule keys", value=", ".join(unknown[:10]), inline=False)
        raw = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
        await interaction.response.send_message(
            embed=embed,
            file=discord.File(io.BytesIO(raw), filename="nmdiscordbot-event-coverage.json"),
            ephemeral=True,
        )


    @app_commands.command(name="topic_triage", description="Изменить статус форумной темы и обновить теги")
    async def topic_triage(self, interaction: discord.Interaction, thread_id: str | None = None, status: str = "открыто", note: str | None = None) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        thread = await self.bot._resolve_thread(resolved_thread_id or '')
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        record = await self.community_store.get_forum_topic(str(thread.id))
        topic_kind = str((record or {}).get('topic_kind') or 'support')
        normalized = normalize_triage_status(status)
        if normalized is None:
            await interaction.response.send_message("Статус должен быть одним из: открыто, на рассмотрении, решено, закрыто.", ephemeral=True)
            return
        await self.community_store.note_forum_staff_response(thread_id=str(thread.id), staff_user_id=str(interaction.user.id), staff_name=str(interaction.user))
        record_metadata = dict((record or {}).get('metadata_json') or {})
        record_metadata['note'] = note or ''
        tags = await self.bot._sync_topic_presentation(thread, topic_kind=topic_kind, status=normalized, metadata=record_metadata, archive_override=(normalized in {'resolved','closed'}))
        await self.bot.record_audit(action='forum_topic_triage', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'thread_id': str(thread.id), 'status': normalized, 'note': note or '', 'tags': tags})
        await interaction.response.send_message(f"Статус темы обновлён: {triage_status_label(normalized)}", ephemeral=True)

    @app_commands.command(name="ops_status", description="Показать операционный статус бота NMDiscordBot")

    async def ops_status(self, interaction: discord.Interaction) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        platform = await self.community_store.health()
        pending_sync = await self.community_store.list_external_sync_events(status='pending', limit=10)
        approvals = await self.community_store.list_approval_requests(status='pending', limit=10)
        panels = await self.community_store.list_panel_bindings(guild_id=str(interaction.guild_id or self.settings.discord_guild_id or ''), limit=10)
        last_cleanup = await self.bot.get_runtime_marker('last_cleanup')
        last_bridge_success = await self.bot.get_runtime_marker('last_bridge_success')
        last_content_reload = await self.bot.get_runtime_marker('last_content_reload')
        last_panel_reconcile = await self.bot.get_runtime_marker('last_panel_reconcile')
        uptime = _format_timedelta_seconds((_utc_now() - self.bot.started_at).total_seconds())
        embed = discord.Embed(title='Операционный статус NMDiscordBot', color=STAFF_COLOR)
        embed.add_field(name='Версия', value=self.bot.version, inline=True)
        embed.add_field(name='Uptime', value=uptime, inline=True)
        embed.add_field(name='Ingress', value='включён' if self.bot.http_ingress.enabled else 'выключен', inline=True)
        embed.add_field(name='Storage', value=str(platform), inline=False)
        embed.add_field(name='Pending sync', value=str(len(pending_sync)), inline=True)
        embed.add_field(name='Pending approvals', value=str(len(approvals)), inline=True)
        embed.add_field(name='Панели', value=str(len(panels)), inline=True)
        if last_bridge_success:
            embed.add_field(name='Последняя успешная bridge-доставка', value=str(last_bridge_success), inline=False)
        if last_cleanup:
            embed.add_field(name='Последний cleanup', value=str(last_cleanup), inline=False)
        if last_content_reload:
            embed.add_field(name='Последний reload контента', value=str(last_content_reload), inline=False)
        if last_panel_reconcile:
            embed.add_field(name='Последний panel reconcile', value=str(last_panel_reconcile), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="bridge_status", description="Показать диагностику доставки bridge-событий")
    @app_commands.default_permissions(manage_guild=True)


    async def bridge_status(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        stats = await self.community_store.get_external_sync_delivery_stats()
        states = {str(row.get('destination') or ''): row for row in await self.community_store.list_bridge_destination_states()}
        if not stats and not states:
            await interaction.response.send_message("Журнал доставки bridge-событий пока пуст.", ephemeral=True)
            return
        embed = discord.Embed(title='Диагностика доставки bridge-событий', color=AUDIT_COLOR)
        grouped: dict[str, list[dict[str, Any]]] = {}
        taxonomy: dict[str, int] = {}
        for row in stats:
            grouped.setdefault(str(row.get('destination') or 'неизвестно'), []).append(row)
            category = _classify_bridge_error(str(row.get('last_error') or ''))
            taxonomy[category] = taxonomy.get(category, 0) + int(row.get('total') or 0)
        destinations = list(dict.fromkeys(list(grouped.keys()) + list(states.keys())))[:10]
        for dest in destinations:
            rows = grouped.get(dest, [])
            state = states.get(dest, {}) if isinstance(states.get(dest, {}), dict) else {}
            sent = sum(int(r.get('total') or 0) for r in rows if str(r.get('status')) == 'sent')
            retry = sum(int(r.get('total') or 0) for r in rows if str(r.get('status')) != 'sent')
            last_error = str(state.get('last_error') or next((str(r.get('last_error') or '') for r in rows if r.get('last_error')), 'нет'))
            last_success = str(state.get('last_success_at') or next((str(r.get('last_success') or '') for r in rows if r.get('last_success')), '—'))
            last_failure = str(state.get('last_failure_at') or next((str(r.get('last_failure') or '') for r in rows if r.get('last_failure')), '—'))
            label = self.bot._bridge_destination_label(dest)
            circuit_state = str(state.get('circuit_state') or 'closed')
            value = (
                f"Отправлено: {sent}\n"
                f"Ожидают повтор: {retry}\n"
                f"Circuit: {circuit_state}\n"
                f"Consecutive failures: {int(state.get('consecutive_failures') or 0)}\n"
                f"Последний успех: {last_success}\n"
                f"Последний сбой: {last_failure}\n"
                f"Ошибка: {last_error[:120]}"
            )
            if state.get('circuit_open_until'):
                value += f"\nCircuit open until: {state.get('circuit_open_until')}"
            embed.add_field(name=label[:256], value=value[:1024], inline=False)
        if taxonomy:
            embed.add_field(name='Таксономия сбоев', value='\n'.join(f'- {k}: {v}' for k, v in sorted(taxonomy.items())), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="topic_status", description="Показать подробный статус форумной темы")
    @app_commands.default_permissions(manage_threads=True)
    async def topic_status(self, interaction: discord.Interaction, thread_id: str | None = None) -> None:
        if not await self._require_scope(interaction, 'support'):
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        if not resolved_thread_id:
            await interaction.response.send_message("Укажи ID темы или вызови команду внутри forum thread.", ephemeral=True)
            return
        record = await self.community_store.get_forum_topic(resolved_thread_id)
        if record is None:
            await interaction.response.send_message("Тема не найдена в реестре бота.", ephemeral=True)
            return
        metadata = dict(record.get('metadata_json') or {})
        owner = metadata.get('staff_owner_user_id')
        owner_ref = f"<@{owner}>" if owner else 'не назначен'
        last_staff = metadata.get('last_staff_response_at') or '—'
        escalated = metadata.get('escalated_at') or '—'
        owner_assigned_at = metadata.get('staff_owner_assigned_at') or '—'
        last_staff_by = metadata.get('last_staff_response_name') or metadata.get('last_staff_response_by') or '—'
        embed = discord.Embed(title='Статус форумной темы', color=AUDIT_COLOR)
        embed.add_field(name='Тема', value=f"<#{record.get('thread_id')}>", inline=False)
        embed.add_field(name='Тип', value=str(record.get('topic_kind') or '—'), inline=True)
        embed.add_field(name='Статус', value=triage_status_label(str(record.get('status') or 'open')), inline=True)
        embed.add_field(name='Источник статуса', value=status_source_label(self.settings.forum_status_source), inline=True)
        embed.add_field(name='Ответственный', value=owner_ref, inline=True)
        embed.add_field(name='Последний ответ staff', value=str(last_staff), inline=True)
        embed.add_field(name='Ответ от staff', value=str(last_staff_by), inline=True)
        embed.add_field(name='Ответственный назначен', value=str(owner_assigned_at), inline=True)
        embed.add_field(name='Эскалация', value=str(escalated), inline=True)
        tags = record.get('tags_json') or []
        if tags:
            embed.add_field(name='Теги', value=', '.join(str(tag) for tag in tags[:10]), inline=False)
        note = str(metadata.get('note') or '').strip()
        if note:
            embed.add_field(name='Примечание', value=note[:500], inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="topics_overdue", description="Показать темы, которые требуют внимания staff")
    @app_commands.default_permissions(manage_threads=True)
    async def topics_overdue(self, interaction: discord.Interaction, limit: app_commands.Range[int, 1, 20] = 10) -> None:
        if not await self._require_scope(interaction, 'support'):
            return
        rows = []
        for kind, hours in [('support', self.settings.support_escalation_hours), ('appeal', self.settings.appeal_escalation_hours), ('report', self.settings.report_escalation_hours)]:
            for row in await self.community_store.list_topics_needing_escalation(topic_kind=kind, older_than_hours=hours, limit=limit):
                row['_overdue_kind'] = kind
                rows.append(row)
        rows = rows[:limit]
        if not rows:
            await interaction.response.send_message('Просроченных тем сейчас нет.', ephemeral=True)
            return
        embed = discord.Embed(title='Темы, требующие внимания', color=STAFF_COLOR)
        for row in rows[:10]:
            md = row.get('metadata_json') or {}
            owner = md.get('staff_owner_user_id')
            owner_ref = f"<@{owner}>" if owner else 'не назначен'
            embed.add_field(name=f"{row.get('_overdue_kind')} • {row.get('thread_id')}", value=(f"Тема: <#{row.get('thread_id')}>\n" f"Ответственный: {owner_ref}\n" f"Обновлено: {row.get('updated_at') or '—'}"), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="bridge_retry_failed", description="Повторно поставить в очередь неотправленные bridge-события")
    async def bridge_retry_failed(self, interaction: discord.Interaction, limit: app_commands.Range[int, 1, 25] = 10, destination: str | None = None) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        rows = await self.community_store.list_failed_external_sync_events(limit=limit, destination=destination)
        if not rows:
            await interaction.response.send_message('Событий для повторной отправки не найдено.', ephemeral=True)
            return
        count = 0
        for row in rows:
            if await self.community_store.requeue_external_sync_event(int(row.get('id') or 0)):
                count += 1
        await self.bot.record_audit(action='bridge_retry_failed', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'count': count, 'destination': destination or '', 'limit': limit})
        await interaction.response.send_message(f"Повторно поставлено в очередь: {count}.", ephemeral=True)

    @app_commands.command(name="bridge_retry_scope", description="Повторить bridge-события по фильтру направления и типа")
    @app_commands.default_permissions(manage_guild=True)
    async def bridge_retry_scope(self, interaction: discord.Interaction, destination: str | None = None, event_kind: str | None = None, hours: app_commands.Range[int, 1, 168] = 24, limit: app_commands.Range[int, 1, 50] = 25) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        rows = await self.community_store.list_failed_external_sync_events(limit=max(limit * 4, 100), destination=destination)
        threshold = _utc_now().timestamp() - (hours * 3600)
        selected = []
        for row in rows:
            if event_kind and event_kind.lower() not in str(row.get('event_kind') or '').lower():
                continue
            updated_dt = _parse_datetime(str(row.get('updated_at') or ''))
            if updated_dt is not None and updated_dt.timestamp() < threshold:
                continue
            selected.append(row)
            if len(selected) >= limit:
                break
        retried = 0
        for row in selected:
            if await self.community_store.requeue_external_sync_event(int(row.get('id') or 0)):
                retried += 1
        await self.bot.record_audit(action='bridge_retry_scope', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'destination': destination or '', 'event_kind': event_kind or '', 'hours': hours, 'selected': len(selected), 'retried': retried})
        await interaction.response.send_message(f'Отобрано событий: {len(selected)}. Повторно поставлено в очередь: {retried}.', ephemeral=True)

    @app_commands.command(name="onboarding_stats", description="Показать статистику по входу и принятию правил")
    async def onboarding_stats(self, interaction: discord.Interaction) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        guild = interaction.guild
        accepted = await self.community_store.get_rules_acceptance_stats(guild_id=str(interaction.guild_id or ''), current_rules_version=self.settings.rules_version)
        visitor_count = 0
        member_count = 0
        if guild is not None:
            if self.settings.visitor_role_id and (role := guild.get_role(self.settings.visitor_role_id)) is not None:
                visitor_count = len(role.members)
            if self.settings.member_role_id and (role := guild.get_role(self.settings.member_role_id)) is not None:
                member_count = len(role.members)
        embed = discord.Embed(title='Статистика входа', color=EMBED_COLOR)
        embed.add_field(name='Версия правил', value=self.settings.rules_version, inline=True)
        embed.add_field(name='Гости', value=str(visitor_count), inline=True)
        embed.add_field(name='Участники', value=str(member_count), inline=True)
        embed.add_field(name='Приняли правила', value=str(accepted.get('total', 0)), inline=True)
        embed.add_field(name='На актуальной версии', value=str(accepted.get('current_version', 0)), inline=True)
        conversion = 0.0
        if visitor_count + member_count > 0:
            conversion = member_count / max(1, visitor_count + member_count)
        embed.add_field(name='Конверсия Visitor → Member', value=f"{conversion:.0%}", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="interest_stats", description="Показать статистику по ролям интересов")
    async def interest_stats(self, interaction: discord.Interaction) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        stats = await self.community_store.get_subscription_preferences_stats(platform='discord')
        mapping = self.bot._interest_role_ids()
        embed = discord.Embed(title='Статистика ролей интересов', color=EMBED_COLOR)
        if not stats:
            embed.description = 'Пока нет данных по подпискам.'
        else:
            for key, role_id in mapping.items():
                embed.add_field(name=key, value=str(stats.get(str(role_id), 0)), inline=True)
        if self.settings.interest_role_ping_map:
            lines = [f"- {key}: {', '.join(value)}" for key, value in self.settings.interest_role_ping_map.items()]
            embed.add_field(name='Маршрутизация упоминаний', value='\n'.join(lines[:10]), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @app_commands.command(name="content_reload", description="Перечитать templates/content.json и при необходимости обновить панели")
    @app_commands.default_permissions(manage_messages=True)


    async def content_reload(self, interaction: discord.Interaction, mode: str = 'apply-and-reconcile') -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        normalized_mode = (mode or 'apply-and-reconcile').strip().lower()
        if normalized_mode not in {'validate-only', 'preview-only', 'apply', 'apply-and-reconcile'}:
            await interaction.response.send_message('Режим должен быть одним из: validate-only, preview-only, apply, apply-and-reconcile.', ephemeral=True)
            return
        old_snapshot = self.bot.load_content_snapshot()
        ensure_content_layout(self.settings)
        issues = validate_content_pack(self.settings)
        new_snapshot = self.bot.load_content_snapshot()
        changed_keys = _diff_content_payloads(old_snapshot, new_snapshot)
        backup_path = None
        if normalized_mode in {'apply', 'apply-and-reconcile'}:
            backup_path = await self.bot.capture_operational_backup(reason='content-reload', actor_user_id=interaction.user.id, guild_id=interaction.guild_id)
        reconciled = False
        if normalized_mode == 'apply-and-reconcile' and interaction.guild_id:
            await self.bot._reconcile_panels(interaction.guild_id)
            await self.bot.set_runtime_marker('last_panel_reconcile', {'at': _format_dt(_utc_now()), 'guild_id': str(interaction.guild_id)})
            reconciled = True
        if normalized_mode in {'apply', 'apply-and-reconcile'}:
            await self.bot.set_runtime_marker('last_content_reload', {'at': _format_dt(_utc_now()), 'changed_keys': changed_keys[:20], 'mode': normalized_mode})
        embed = discord.Embed(title='Контент-пак обработан', color=EMBED_COLOR if not issues else ERROR_COLOR)
        embed.add_field(name='Файл', value=str(self.settings.discord_content_file_path), inline=False)
        embed.add_field(name='Версия схемы', value=str(content_schema_version(self.settings)), inline=True)
        embed.add_field(name='Режим', value=normalized_mode, inline=True)
        embed.add_field(name='Панели обновлены', value='да' if reconciled else 'нет', inline=True)
        embed.add_field(name='Изменённых ключей', value=str(len(changed_keys)), inline=True)
        if changed_keys:
            embed.add_field(name='Diff-preview', value='\n'.join(f'- {item}' for item in changed_keys[:12]), inline=False)
        if backup_path:
            embed.add_field(name='Резервная копия', value=str(backup_path), inline=False)
        if issues:
            embed.add_field(name='Предупреждения', value='\n'.join(f'- {i}' for i in issues[:8]), inline=False)
        await self.bot.record_audit(action='content_reload', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'changed_keys': changed_keys[:20], 'issues': issues[:8], 'backup_path': str(backup_path) if backup_path else '', 'mode': normalized_mode})
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="panel_preview", description="Показать предварительный просмотр панели без публикации")
    @app_commands.choices(panel=[
        app_commands.Choice(name="панель входа", value="onboarding"),
        app_commands.Choice(name="панель ролей интересов", value="interest_roles"),
        app_commands.Choice(name="панель навигации", value="help"),
    ])
    async def panel_preview(self, interaction: discord.Interaction, panel: app_commands.Choice[str]) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        from .discord_panels import build_panel_preview_embed
        embed = build_panel_preview_embed(self.settings, panel.value)
        embed.add_field(name='Версия панели', value=get_panel_version(self.settings, panel.value), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @app_commands.command(name="panel_restore", description="Точечно перепубликовать одну панель")
    @app_commands.choices(panel=[
        app_commands.Choice(name="панель входа", value="onboarding"),
        app_commands.Choice(name="панель ролей интересов", value="interest_roles"),
        app_commands.Choice(name="панель навигации", value="help"),
    ])
    async def panel_restore(self, interaction: discord.Interaction, panel: app_commands.Choice[str]) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        if not interaction.guild_id:
            await interaction.response.send_message('Команду нужно запускать на сервере.', ephemeral=True)
            return
        backup_path = await self.bot.capture_operational_backup(reason=f'panel-restore-{panel.value}', actor_user_id=interaction.user.id, guild_id=interaction.guild_id)
        message = await self.bot.publish_panel(guild_id=interaction.guild_id, panel_type=panel.value, actor_user_id=interaction.user.id)
        await self.bot.set_runtime_marker('last_panel_reconcile', {'at': _format_dt(_utc_now()), 'guild_id': str(interaction.guild_id), 'reason': f'panel_restore:{panel.value}'})
        await self.bot.record_audit(action='panel_restore', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'panel_type': panel.value, 'message_id': message.id, 'backup_path': str(backup_path) if backup_path else ''})
        await interaction.response.send_message(f'Панель восстановлена: {panel.value}.', ephemeral=True)

    @app_commands.command(name="topic_export", description="Скачать транскрипт forum-темы")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(thread_id=_autocomplete_thread_id)
    @app_commands.describe(mode="auto/staff/public/metadata")
    async def topic_export(self, interaction: discord.Interaction, thread_id: str | None = None, mode: str = 'auto') -> None:
        if not await self._require_scope(interaction, 'support'):
            return
        normalized_mode = (mode or 'auto').strip().lower()
        if normalized_mode not in {'auto','staff','public','metadata'}:
            await interaction.response.send_message("Режим должен быть одним из: auto, staff, public, metadata.", ephemeral=True)
            return
        resolved_thread_id = self._resolve_target_thread_id(interaction, thread_id)
        thread = await self.bot._resolve_thread(resolved_thread_id or '')
        if thread is None:
            await interaction.response.send_message("Тема не найдена.", ephemeral=True)
            return
        if normalized_mode == 'auto':
            record = await self.community_store.get_forum_topic(str(thread.id))
            topic_kind = str((record or {}).get('topic_kind') or 'support')
            normalized_mode = str(self._forum_policy(topic_kind).get('export_mode') or 'auto').strip().lower()
            if normalized_mode == 'auto':
                if topic_kind in {'appeal','report'}:
                    normalized_mode = 'staff'
                elif topic_kind == 'guild_recruitment':
                    normalized_mode = 'public'
                else:
                    normalized_mode = 'metadata' if topic_kind == 'support' else 'staff'
        filename, payload = await self._build_thread_transcript(thread, mode=normalized_mode)
        file = discord.File(io.BytesIO(payload), filename=filename)
        await self.bot.record_audit(action='topic_export', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'thread_id': str(thread.id), 'mode': normalized_mode})
        await interaction.response.send_message("Транскрипт темы сформирован.", file=file, ephemeral=True)

    @app_commands.command(name="scheduled_jobs", description="Показать запланированные напоминания и задания")
    @app_commands.default_permissions(manage_guild=True)


    async def scheduled_jobs(self, interaction: discord.Interaction, limit: app_commands.Range[int,1,20] = 10) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        pending = await self.community_store.list_scheduled_jobs(statuses=('pending','retry'), limit=max(limit, 25))
        failed = await self.community_store.list_scheduled_jobs(statuses=('failed',), limit=max(limit, 25))
        sent = await self.community_store.list_scheduled_jobs(statuses=('sent',), limit=max(limit, 25))
        embed = discord.Embed(title='Запланированные задания', color=STAFF_COLOR)
        embed.add_field(name='Ожидают', value=str(len(pending)), inline=True)
        embed.add_field(name='Ошибки', value=str(len(failed)), inline=True)
        embed.add_field(name='Завершено (видимое окно)', value=str(len(sent)), inline=True)
        rows = pending[:limit]
        if not rows:
            embed.description = 'Активных заданий нет.'
        else:
            for row in rows[:10]:
                label = f"#{row.get('id')} • {row.get('job_type')} • {row.get('status')}"
                value = f"Когда: {row.get('run_at')}"
                if str(row.get('channel_id') or '').isdigit():
                    value += f"\nКанал: <#{row.get('channel_id')}>"
                if row.get('updated_at'):
                    value += f"\nОбновлено: {row.get('updated_at')}"
                embed.add_field(name=label, value=value, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="scheduled_job_cancel", description="Отменить запланированное задание")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(job_id=_autocomplete_job_id)
    async def scheduled_job_cancel(self, interaction: discord.Interaction, job_id: int) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        ok = await self.community_store.cancel_scheduled_job(job_id)
        await interaction.response.send_message("Задание отменено." if ok else "Задание не найдено или уже завершено.", ephemeral=True)

    @app_commands.command(name="scheduled_job_reschedule", description="Перенести время запланированного задания")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(job_id=_autocomplete_job_id)
    async def scheduled_job_reschedule(self, interaction: discord.Interaction, job_id: int, run_at: str) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        dt = _parse_datetime(run_at)
        if dt is None:
            await interaction.response.send_message("Укажи время в ISO-формате.", ephemeral=True)
            return
        ok = await self.community_store.reschedule_scheduled_job(job_id, run_at=_format_dt(dt))
        await interaction.response.send_message("Время задания обновлено." if ok else "Задание не найдено.", ephemeral=True)

    @app_commands.command(name="scheduled_job_run", description="Принудительно выполнить запланированное задание")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(job_id=_autocomplete_job_id)
    async def scheduled_job_run(self, interaction: discord.Interaction, job_id: int) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        row = await self.community_store.get_scheduled_job(job_id)
        if row is None:
            await interaction.response.send_message("Задание не найдено.", ephemeral=True)
            return
        payload = row.get('payload_json') or {}
        if str(row.get('job_type') or '') == 'stage_announce':
            await self.bot._send_stage_announcement(title=str(payload.get('title') or 'NeverMine stage'), description=str(payload.get('description') or ''), starts_at=str(payload.get('starts_at') or ''), actor_user_id=interaction.user.id)
        elif str(row.get('job_type') or '') == 'event_reminder':
            await self.bot._send_event_reminder(title=str(payload.get('title') or 'NeverMine event'), description=str(payload.get('description') or ''), starts_at=str(payload.get('starts_at') or ''), actor_user_id=interaction.user.id)
        await self.community_store.mark_scheduled_job(job_id, status='sent')
        await interaction.response.send_message('Задание выполнено.', ephemeral=True)

    @app_commands.command(name="bridge_event_status", description="Показать детали bridge-события по ID")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(event_id=_autocomplete_event_id)
    async def bridge_event_status(self, interaction: discord.Interaction, event_id: int) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        row = await self.community_store.get_external_sync_event(event_id)
        if row is None:
            await interaction.response.send_message("Событие не найдено.", ephemeral=True)
            return
        embed = discord.Embed(title=f'Bridge-событие #{event_id}', color=STAFF_COLOR)
        embed.add_field(name='Назначение', value=str(row.get('destination') or '—'), inline=False)
        embed.add_field(name='Тип', value=str(row.get('event_kind') or row.get('kind') or '—'), inline=True)
        embed.add_field(name='Статус', value=str(row.get('status') or '—'), inline=True)
        embed.add_field(name='Обновлено', value=str(row.get('updated_at') or '—'), inline=True)
        if row.get('last_error'):
            embed.add_field(name='Последняя ошибка', value=str(row.get('last_error'))[:800], inline=False)
        embed.add_field(name='Payload', value=_preview_payload(_sanitize_payload(row.get('payload_json') or {}), max_length=1500), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="bridge_event_retry", description="Повторить одно bridge-событие по ID")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(event_id=_autocomplete_event_id)
    async def bridge_event_retry(self, interaction: discord.Interaction, event_id: int) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        ok = await self.community_store.requeue_external_sync_event(event_id)
        await interaction.response.send_message('Событие повторно поставлено в очередь.' if ok else 'Событие не найдено.', ephemeral=True)

    @app_commands.command(name="layout_repair", description="Проверить и при необходимости починить layout Discord-сервера")
    @app_commands.default_permissions(manage_guild=True)
    async def layout_repair(self, interaction: discord.Interaction, apply: bool = False, confirm: bool = False, scope: str = 'all') -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        guild = interaction.guild or (self.bot.get_guild(self.settings.discord_guild_id) if self.settings.discord_guild_id else None)
        if guild is None:
            await interaction.response.send_message('Не удалось определить сервер.', ephemeral=True)
            return
        normalized_scope = (scope or 'all').strip().lower()
        allowed_scopes = {'all','forums','panels','tags','roles','channels','readonly','drift','only-missing-tags','only-missing-panels','only-permission-check','only-republish-panels'}
        if normalized_scope not in allowed_scopes:
            await interaction.response.send_message('scope должен быть одним из: ' + ', '.join(sorted(allowed_scopes)) + '.', ephemeral=True)
            return
        spec = load_server_layout(ensure_server_layout_file())
        issues = validate_server_layout(spec)
        drift = await _collect_layout_drift(self.bot, guild)
        planned = _summarize_layout_drift(drift, normalized_scope)
        if apply and not confirm:
            await interaction.response.send_message('Для применения исправлений повторите команду с confirm=true. Сейчас показан только dry-run.', ephemeral=True)
            return
        if not apply:
            payload = {'scope': normalized_scope, 'layout_issues': issues, 'drift': drift}
            data = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode('utf-8')
            message = 'Dry-run layout repair\n' + ('\n'.join(f'- {item}' for item in planned) if planned else '- drift не обнаружен')
            await interaction.response.send_message(message, file=discord.File(io.BytesIO(data), filename='nmdiscord-layout-drift.json'), ephemeral=True)
            return
        backup_path = await self.bot.capture_operational_backup(reason='layout-repair', actor_user_id=interaction.user.id, guild_id=interaction.guild_id)
        fixes = await _apply_layout_repair(self.bot, guild, drift, normalized_scope)
        if normalized_scope in {'all','panels','only-missing-panels','only-republish-panels'}:
            await self.bot._reconcile_panels(guild.id)
            fixes.append('панели перепроверены и обновлены')
            await self.bot.set_runtime_marker('last_panel_reconcile', {'at': _format_dt(_utc_now()), 'guild_id': str(guild.id), 'reason': 'layout_repair'})
        await self.bot.record_audit(action='layout_repair', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'apply': apply, 'confirm': confirm, 'scope': normalized_scope, 'planned': planned, 'fixes': fixes, 'backup_path': str(backup_path) if backup_path else '', 'layout_issues': issues[:20]})
        await interaction.response.send_message('Исправления применены: ' + (', '.join(fixes) if fixes else 'изменения не потребовались') + (f'\nРезервная копия: {backup_path}' if backup_path else ''), ephemeral=True)

    @app_commands.command(name="cleanup_status", description="Показать последние результаты cleanup и автоархивации")
    @app_commands.default_permissions(manage_guild=True)

    async def cleanup_status(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        summary = await self.storage.database.get_key_value('cleanup_last_summary') if hasattr(self.storage.database, 'get_key_value') else None
        embed = discord.Embed(title='Состояние cleanup', color=STAFF_COLOR)
        if not summary:
            embed.description = 'Данные о последнем цикле очистки пока недоступны.'
        else:
            for key in ('executed_at','deleted','stale_topics_closed','sqlite_actions','escalations','warnings','scheduled_jobs_pruned','bridge_events_pruned'):
                if key in summary:
                    embed.add_field(name=key, value=str(summary.get(key)), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @app_commands.command(name="history_snapshot", description="Показать сводку по bridge и заданиям за последнее окно")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(destination=_autocomplete_destination)
    async def history_snapshot(self, interaction: discord.Interaction, destination: str | None = None, event_kind: str | None = None, hours: app_commands.Range[int,1,168] = 24) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        failed = await self.community_store.list_failed_external_sync_events(limit=250, destination=destination, event_kind=event_kind, since_hours=hours)
        jobs_pending = await self.community_store.list_scheduled_jobs(statuses=('pending','retry'), limit=250, since_hours=hours)
        jobs_terminal = await self.community_store.list_scheduled_jobs(statuses=('failed','sent','cancelled'), limit=250, since_hours=hours)
        stats = await self.community_store.get_external_sync_delivery_stats(since_hours=hours)
        trends: dict[str, int] = {}
        kind_trends: dict[str, int] = {}
        for row in failed:
            dst = self.bot._bridge_destination_label(str(row.get('destination') or ''))
            kind = str(row.get('event_kind') or row.get('kind') or 'unknown')
            trends[dst] = trends.get(dst, 0) + 1
            kind_trends[kind] = kind_trends.get(kind, 0) + 1
        embed = discord.Embed(title='История bridge и заданий', color=STAFF_COLOR)
        embed.add_field(name='Проваленные bridge-события', value=str(len(failed)), inline=True)
        embed.add_field(name='Задания pending/retry', value=str(len(jobs_pending)), inline=True)
        embed.add_field(name='Задания terminal', value=str(len(jobs_terminal)), inline=True)
        if stats:
            lines = []
            for row in stats[:10]:
                label = self.bot._bridge_destination_label(str(row.get('destination') or ''))
                lines.append(f"- {label}: {row.get('status')}={row.get('total')}")
            embed.add_field(name='Доставка по направлениям', value='\n'.join(lines), inline=False)
        if trends:
            embed.add_field(name='Провалы по направлениям', value='\n'.join(f'- {k}: {v}' for k, v in list(trends.items())[:10]), inline=False)
        if kind_trends:
            embed.add_field(name='Провалы по типам', value='\n'.join(f'- {k}: {v}' for k, v in list(kind_trends.items())[:10]), inline=False)
        embed.add_field(name='Окно', value=f'{hours} ч.', inline=True)
        if destination:
            embed.add_field(name='Назначение', value=destination, inline=True)
        if event_kind:
            embed.add_field(name='Тип события', value=event_kind, inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="staff_scope_map", description="Показать эффективную карту staff-scopes")
    @app_commands.default_permissions(manage_guild=True)
    async def staff_scope_map(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        mapping = self.settings.staff_scope_role_map or {}
        embed = discord.Embed(title='Карта staff-scopes', color=STAFF_COLOR)
        if not mapping:
            embed.description = 'Дополнительная карта ролей по scope пока не настроена.'
        else:
            for scope, roles in mapping.items():
                embed.add_field(name=scope, value=', '.join(roles) if roles else 'нет ролей', inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="boot_diagnostics", description="Показать диагностику запуска, схемы и интеграций")
    @app_commands.default_permissions(manage_guild=True)
    async def boot_diagnostics(self, interaction: discord.Interaction) -> None:
        if not self.bot.is_staff(interaction.user):
            await interaction.response.send_message("Недостаточно прав.", ephemeral=True)
            return
        platform = await self.community_store.health()
        embed = discord.Embed(title='Диагностика запуска', color=STAFF_COLOR)
        embed.add_field(name='Версия', value=self.bot.version, inline=True)
        embed.add_field(name='Schema', value=str(await self.community_store.get_schema_version() if hasattr(self.community_store,'get_schema_version') else self.storage.schema_version), inline=True)
        embed.add_field(name='Схема контента', value=str(content_schema_version(self.settings)), inline=True)
        embed.add_field(name='Хранилище', value=str(platform), inline=False)
        embed.add_field(name='Ingress', value='включён' if self.bot.http_ingress.enabled else 'выключен', inline=True)
        embed.add_field(name='Назначения bridge', value=', '.join(self.bot._bridge_destinations()) or 'не настроены', inline=False)
        embed.add_field(name='Контент', value=str(self.settings.discord_content_file_path), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


    @app_commands.command(name="topic_form", description="Открыть модальную форму для новой forum-темы")
    async def topic_form(self, interaction: discord.Interaction, kind: str = 'support') -> None:
        kind = (kind or 'support').strip().lower()
        mapping = {
            'support': (self.settings.discord_forum_help_channel_id, 'support', 'Новая тема поддержки'),
            'bug': (self.settings.discord_forum_bug_reports_channel_id, 'bug', 'Новый баг-репорт'),
            'suggestion': (self.settings.discord_forum_suggestions_channel_id, 'suggestion', 'Новое предложение'),
            'appeal': (self.settings.discord_forum_appeals_channel_id, 'appeal', 'Новая апелляция'),
        }
        forum_id, topic_kind, title = mapping.get(kind, mapping['support'])
        await interaction.response.send_modal(TopicCreateModal(self, topic_kind=topic_kind, forum_channel_id=forum_id, title=title))

    @app_commands.command(name="staff_digest_now", description="Отправить сводку staff по инцидентам")
    @app_commands.default_permissions(manage_guild=True)
    async def staff_digest_now(self, interaction: discord.Interaction, channel_id: str | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        destination = self.bot._get_message_channel(int(channel_id)) if channel_id and channel_id.isdigit() else self.bot._get_message_channel(self.settings.discord_ops_audit_channel_id or self.settings.discord_audit_channel_id)
        if destination is None:
            await interaction.response.send_message('Канал для сводки не настроен.', ephemeral=True)
            return
        sent = await self.bot.send_staff_digest(channel=destination, actor_user_id=interaction.user.id)
        await interaction.response.send_message('Сводка отправлена.' if sent else get_ops_text(self.settings, 'staff_digest_empty', 'Критичных инцидентов нет.'), ephemeral=True)

    @app_commands.command(name="staff_digest_schedule", description="Запланировать отправку staff-сводки")
    @app_commands.default_permissions(manage_guild=True)
    async def staff_digest_schedule(self, interaction: discord.Interaction, hours_from_now: app_commands.Range[int,1,168] = 24, repeat_every_hours: int = 0, repeat_count: int = 0) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        if repeat_every_hours < 0 or repeat_every_hours > 168 or repeat_count < 0 or repeat_count > 365:
            await interaction.response.send_message('repeat_every_hours должен быть в диапазоне 0..168, repeat_count — 0..365.', ephemeral=True)
            return
        channel_id = str(self.settings.discord_ops_audit_channel_id or self.settings.discord_audit_channel_id or '')
        if not channel_id:
            await interaction.response.send_message('Канал для сводки не настроен.', ephemeral=True)
            return
        dt = _utc_now() + timedelta(hours=int(hours_from_now))
        payload = build_digest_schedule_payload(digest_kind='staff', recurrence_hours=repeat_every_hours or None, remaining_occurrences=repeat_count or None, digest_scope='staff')
        run_at = _format_dt(dt)
        dedupe_key = build_scheduled_job_dedupe_key(job_type='staff_digest', guild_id=str(interaction.guild_id or ''), channel_id=channel_id, run_at=run_at, payload=payload)
        job_id = await self.community_store.schedule_job(job_type='staff_digest', run_at=run_at, payload=payload, guild_id=str(interaction.guild_id or ''), channel_id=channel_id, created_by=str(interaction.user.id), dedupe_key=dedupe_key)
        await self.bot.record_audit(action='staff_digest_scheduled', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'job_id': job_id, 'run_at': run_at, 'channel_id': channel_id, 'recurrence': recurrence_summary(recurrence_hours=repeat_every_hours, remaining_occurrences=repeat_count or None)})
        await interaction.response.send_message(f'Staff-сводка запланирована на {run_at} ({recurrence_summary(recurrence_hours=repeat_every_hours, remaining_occurrences=repeat_count or None)}).', ephemeral=True)

    @app_commands.command(name="staff_digest_calendar", description="Запланировать staff-сводку по календарю")
    @app_commands.default_permissions(manage_guild=True)
    async def staff_digest_calendar(self, interaction: discord.Interaction, local_time: str = '09:00', weekday: str | None = None, timezone_name: str = 'Europe/Berlin', repeat_count: int = 0, weekday_set: str | None = None, day_of_month: app_commands.Range[int,1,28] | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        if repeat_count < 0 or repeat_count > 365:
            await interaction.response.send_message('repeat_count должен быть в диапазоне 0..365.', ephemeral=True)
            return
        channel_id = str(self.settings.discord_ops_audit_channel_id or self.settings.discord_audit_channel_id or '')
        if not channel_id:
            await interaction.response.send_message('Канал для сводки не настроен.', ephemeral=True)
            return
        payload = build_calendar_schedule_payload(digest_kind='staff', digest_scope='staff', timezone_name=timezone_name, local_time=local_time, weekday=weekday or None, weekday_set=weekday_set or None, day_of_month=int(day_of_month) if day_of_month else None, remaining_occurrences=repeat_count or None)
        run_at = first_calendar_run_at(payload=payload)
        dedupe_key = build_scheduled_job_dedupe_key(job_type='staff_digest', guild_id=str(interaction.guild_id or ''), channel_id=channel_id, run_at=run_at, payload=payload)
        job_id = await self.community_store.schedule_job(job_type='staff_digest', run_at=run_at, payload=payload, guild_id=str(interaction.guild_id or ''), channel_id=channel_id, created_by=str(interaction.user.id), dedupe_key=dedupe_key)
        summary = recurrence_summary(calendar_mode=str(payload.get('calendar_mode') or ''), calendar_time=str(payload.get('calendar_time') or ''), calendar_weekday=str(payload.get('calendar_weekday') or ''), calendar_weekdays=list(payload.get('calendar_weekdays') or []), calendar_day_of_month=int(payload.get('calendar_day_of_month') or 0) or None, timezone_name=str(payload.get('calendar_timezone') or ''), remaining_occurrences=repeat_count or None)
        await self.bot.record_audit(action='staff_digest_calendar_scheduled', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'job_id': job_id, 'run_at': run_at, 'channel_id': channel_id, 'recurrence': summary})
        await interaction.response.send_message(f'Staff-сводка запланирована на {run_at} ({summary}).', ephemeral=True)

    @app_commands.command(name="maintenance_mode", description="Включить или выключить режим технических работ")
    @app_commands.default_permissions(manage_guild=True)

    async def maintenance_mode(self, interaction: discord.Interaction, enabled: bool, message: str | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        backup_path = await self.bot.capture_operational_backup(reason='maintenance-mode', actor_user_id=interaction.user.id, guild_id=interaction.guild_id)
        state = {'enabled': bool(enabled), 'message': (message or self.settings.maintenance_mode_message).strip()}
        self.bot.maintenance_mode = state
        await self.storage.database.set_key_value('maintenance_mode', state)
        await self.bot.set_runtime_marker('last_maintenance_mode_change', {'at': _format_dt(_utc_now()), 'enabled': bool(enabled), 'actor_user_id': str(interaction.user.id)})
        await self.bot.record_audit(action='maintenance_mode_changed', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={**state, 'backup_path': str(backup_path) if backup_path else ''})
        await interaction.response.send_message('Режим технических работ обновлён.', ephemeral=True)

    @app_commands.command(name="forum_policy_view", description="Показать runtime-политику forum-тем")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(topic_kind=_autocomplete_topic_kind)
    async def forum_policy_view(self, interaction: discord.Interaction, topic_kind: str) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        policy = self._forum_policy(topic_kind.strip().lower())
        embed = discord.Embed(title='Политика forum-тем', color=STAFF_COLOR)
        embed.add_field(name='Тип', value=topic_kind, inline=True)
        for key, value in policy.items():
            embed.add_field(name=key, value=str(value), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="forum_policy_set", description="Изменить runtime-политику forum-тем")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(topic_kind=_autocomplete_topic_kind)
    async def forum_policy_set(self, interaction: discord.Interaction, topic_kind: str, field: str, value: str, confirm: bool = False) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        allowed_fields = {'auto_close_after_seconds', 'escalation_hours', 'duplicate_detection_hours', 'attachment_max_bytes', 'export_mode'}
        if field not in allowed_fields:
            await interaction.response.send_message('Поле должно быть одним из: auto_close_after_seconds, escalation_hours, duplicate_detection_hours, attachment_max_bytes, export_mode.', ephemeral=True)
            return
        current_policy = self._forum_policy(topic_kind)
        old_value = current_policy.get(field)
        proposed = value
        if field != 'export_mode':
            try:
                proposed = int(value)
            except Exception:
                await interaction.response.send_message('Для этого поля нужно целое число.', ephemeral=True)
                return
        else:
            proposed = str(value).strip().lower()
            if proposed not in {'staff', 'public', 'metadata'}:
                await interaction.response.send_message('export_mode должен быть одним из: staff, public, metadata.', ephemeral=True)
                return
        if not confirm:
            await interaction.response.send_message(f'Предпросмотр изменения: {topic_kind}.{field}: {old_value} → {proposed}. Повторите команду с confirm=true для применения.', ephemeral=True)
            return
        backup_path = await self.bot.capture_operational_backup(reason='forum-policy-set', actor_user_id=interaction.user.id, guild_id=interaction.guild_id)
        overrides = dict(getattr(self.bot, 'runtime_forum_policy_overrides', {}) or {})
        current = dict(overrides.get(topic_kind, {}) or {})
        current[field] = proposed
        overrides[topic_kind] = current
        self.bot.runtime_forum_policy_overrides = overrides
        await self.storage.database.set_key_value('runtime_forum_policy_overrides', overrides)
        await self.bot.set_runtime_marker('last_forum_policy_change', {'at': _format_dt(_utc_now()), 'topic_kind': topic_kind, 'field': field, 'value': proposed, 'actor_user_id': str(interaction.user.id)})
        await self.bot.record_audit(action='forum_policy_changed', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'topic_kind': topic_kind, 'field': field, 'old_value': old_value, 'value': proposed, 'backup_path': str(backup_path) if backup_path else ''})
        await interaction.response.send_message('Политика обновлена.', ephemeral=True)

    @app_commands.command(name="cleanup_preview", description="Показать предварительный результат следующего cleanup")
    @app_commands.default_permissions(manage_guild=True)
    async def cleanup_preview(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        stale_support = await self.community_store.list_topics_needing_escalation(topic_kind='support', older_than_hours=self.settings.support_escalation_hours, limit=20)
        stale_appeals = await self.community_store.list_topics_needing_escalation(topic_kind='appeal', older_than_hours=self.settings.appeal_escalation_hours, limit=20)
        jobs = await self.community_store.list_scheduled_jobs(limit=50)
        embed = discord.Embed(title='Предпросмотр cleanup', color=STAFF_COLOR)
        embed.add_field(name='Support-темы для реакции', value=str(len(stale_support)), inline=True)
        embed.add_field(name='Апелляции для реакции', value=str(len(stale_appeals)), inline=True)
        embed.add_field(name='Запланированные задания', value=str(len(jobs)), inline=True)
        embed.add_field(name='Retention audit', value=f"{self.settings.audit_log_retention_days} дн.", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="ingress_keys", description="Показать состояние ingress-ключей")
    @app_commands.default_permissions(manage_guild=True)
    async def ingress_keys(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        embed = discord.Embed(title='Ingress-ключи', color=STAFF_COLOR)
        embed.add_field(name='ID активного исходящего ключа', value=self.settings.outbound_key_id or 'v1', inline=True)
        embed.add_field(name='Текущий ingress HMAC', value='настроен' if self.settings.ingress_hmac_secret else 'не настроен', inline=True)
        embed.add_field(name='Предыдущий ingress HMAC', value='настроен' if self.settings.ingress_previous_hmac_secret else 'не настроен', inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="state_export", description="Выгрузить operational state бота в JSON")
    @app_commands.default_permissions(manage_guild=True)
    async def state_export(self, interaction: discord.Interaction) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        payload = await self.bot.export_operational_state(guild_id=str(interaction.guild_id or self.settings.discord_guild_id or ''))
        payload['exported_at'] = _format_dt(_utc_now())
        payload['runtime_version'] = self.bot.version
        payload['state_version'] = 7
        payload['build_version'] = self.bot.version
        payload['content_schema_version'] = self.settings.content_schema_version_required
        payload['export_kind'] = 'operational_state'
        payload['snapshot_kind'] = 'state_export'
        payload['server_layout'] = load_server_layout(ensure_server_layout_file())
        payload['restore_capabilities'] = restore_capability_sections()
        preview = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode('utf-8')
        payload['snapshot_sha256'] = hashlib.sha256(preview).hexdigest()
        data = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode('utf-8')
        await interaction.response.send_message('Состояние выгружено.', file=discord.File(io.BytesIO(data), filename='nmdiscord-state.json'), ephemeral=True)

    @app_commands.command(name="state_restore", description="Безопасично восстановить часть operational state из JSON")
    @app_commands.default_permissions(manage_guild=True)
    async def state_restore(self, interaction: discord.Interaction, snapshot: discord.Attachment, section: str = 'all', dry_run: bool = True) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        if snapshot.size > 2_000_000:
            await interaction.response.send_message('Снимок слишком большой.', ephemeral=True)
            return
        try:
            payload = json.loads((await snapshot.read()).decode('utf-8'))
        except Exception:
            await interaction.response.send_message('Не удалось прочитать JSON-снимок.', ephemeral=True)
            return
        section = (section or 'all').strip().lower()
        allowed = {'all', 'maintenance', 'forum_policies', 'panel_registry', 'scheduled_jobs', 'layout_alias_bindings', 'bridge_destination_state', 'schema_meta', 'schema_meta_ledger', 'runtime_markers', 'topics', 'failed_bridge_events', 'panel_drift', 'diagnostics', 'content_pack_meta', 'layout_spec_meta', 'runtime_markers_snapshot', 'build_metadata', 'bridge_comment_mirror', 'external_discussion_mirror', 'external_content_mirror'}
        if section not in allowed:
            await interaction.response.send_message('Раздел должен быть одним из: ' + ', '.join(sorted(allowed)) + '.', ephemeral=True)
            return
        snapshot_version = int(payload.get('state_version') or 0)
        if snapshot_version and snapshot_version > 7:
            await interaction.response.send_message('Снимок создан более новой версией и может быть несовместим с текущим runtime.', ephemeral=True)
            return
        plan = _build_state_restore_plan(payload, section)
        if dry_run:
            data = json.dumps({'section': section, 'plan': plan}, ensure_ascii=False, indent=2, default=str).encode('utf-8')
            await interaction.response.send_message('Dry-run восстановления: ' + (', '.join(plan.get('sections') or []) if plan.get('sections') else 'ничего не будет изменено') + '.', file=discord.File(io.BytesIO(data), filename='nmdiscord-state-restore-plan.json'), ephemeral=True)
            return
        request_id = await self._queue_risky_approval(interaction, kind='state_restore', payload={'snapshot': payload, 'section': section, 'guild_id': str(interaction.guild_id or self.settings.discord_guild_id or '')}, summary=f'state_restore:{section}')
        await interaction.response.send_message(f'Создан запрос на согласование восстановления состояния: №{request_id}.', ephemeral=True)

    @app_commands.command(name="bridge_retry_quick", description="Быстро повторить failed bridge-события по направлению")
    @app_commands.default_permissions(manage_guild=True)
    async def bridge_retry_quick(self, interaction: discord.Interaction, destination: str, limit: app_commands.Range[int, 1, 50] = 10) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        rows = await self.community_store.list_failed_external_sync_events(destination=destination, limit=limit)
        retried = 0
        for row in rows:
            if await self.community_store.requeue_external_sync_event(int(row.get('id'))):
                retried += 1
        await self.bot.record_audit(action='bridge_retry_quick', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'destination': destination, 'limit': limit, 'retried': retried})
        await interaction.response.send_message(f'Повторно поставлено в очередь: {retried}.', ephemeral=True)



    @app_commands.command(name="bridge_dead_letters", description="Показать bridge-события, попавшие в DLQ")
    @app_commands.default_permissions(manage_guild=True)

    async def bridge_dead_letters(self, interaction: discord.Interaction, limit: app_commands.Range[int,1,25] = 10, destination: str | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        rows = await self.community_store.list_failed_external_sync_events(limit=max(limit * 5, 50), destination=destination)
        threshold = _utc_now().timestamp() - (self.settings.bridge_dlq_after_hours * 3600)
        selected=[]
        for row in rows:
            updated_dt = _parse_datetime(str(row.get('updated_at') or ''))
            if updated_dt and updated_dt.timestamp() > threshold:
                continue
            if not str(row.get('last_error') or '').strip():
                continue
            selected.append(row)
            if len(selected) >= limit:
                break
        embed = discord.Embed(title='Bridge DLQ', color=ERROR_COLOR if selected else STAFF_COLOR)
        if not selected:
            embed.description = 'Bridge-событий в DLQ не найдено.'
        else:
            for row in selected:
                value = (
                    f"Тип: {row.get('event_kind')}\n"
                    f"Класс ошибки: {_classify_bridge_error(str(row.get('last_error') or ''))}\n"
                    f"Обновлено: {row.get('updated_at')}\n"
                    f"Ошибка: {str(row.get('last_error') or '')[:120]}"
                )
                embed.add_field(name=f"#{row.get('id')} • {self.bot._bridge_destination_label(str(row.get('destination') or ''))}", value=value, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="bridge_dead_letter_requeue", description="Вернуть DLQ bridge-события в очередь")
    @app_commands.default_permissions(manage_guild=True)
    async def bridge_dead_letter_requeue(self, interaction: discord.Interaction, limit: app_commands.Range[int,1,25] = 10, destination: str | None = None) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        request_id = await self._queue_risky_approval(interaction, kind='bridge_dead_letter_requeue', payload={'limit': int(limit), 'destination': destination or '', 'hours': int(self.settings.bridge_dlq_after_hours), 'guild_id': str(interaction.guild_id or self.settings.discord_guild_id or '')}, summary=f'bridge_dead_letter_requeue:{destination or "all"}:{limit}')
        await interaction.response.send_message(f'Создан запрос на согласование повторной постановки DLQ bridge-событий: №{request_id}.', ephemeral=True)

    @app_commands.command(name="job_dead_letters", description="Показать задания scheduler, попавшие в DLQ")
    @app_commands.default_permissions(manage_guild=True)

    async def job_dead_letters(self, interaction: discord.Interaction, limit: app_commands.Range[int,1,25] = 10) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        rows = await self.community_store.list_scheduled_jobs(statuses=('failed','retry','dead_letter','cancelled'), limit=max(limit * 5, 50))
        threshold = _utc_now().timestamp() - (self.settings.job_dlq_after_hours * 3600)
        selected=[]
        for row in rows:
            updated_dt = _parse_datetime(str(row.get('updated_at') or ''))
            if updated_dt and updated_dt.timestamp() > threshold:
                continue
            if str(row.get('status') or '') not in {'failed','retry','dead_letter','cancelled'}:
                continue
            selected.append(row)
            if len(selected) >= limit:
                break
        embed = discord.Embed(title='Scheduler DLQ', color=ERROR_COLOR if selected else STAFF_COLOR)
        if not selected:
            embed.description = 'Заданий в DLQ не найдено.'
        else:
            for row in selected:
                value = (
                    f"Статус: {row.get('status')}\n"
                    f"Когда: {row.get('run_at')}\n"
                    f"Ошибка: {str(row.get('last_error') or 'нет')[:120]}"
                )
                embed.add_field(name=f"#{row.get('id')} • {row.get('job_type')}", value=value, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="job_dead_letter_requeue", description="Вернуть задания из DLQ в очередь")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.autocomplete(job_id=_autocomplete_job_id)
    async def job_dead_letter_requeue(self, interaction: discord.Interaction, job_id: int | None = None, limit: app_commands.Range[int,1,25] = 10) -> None:
        if not await self._require_scope(interaction, 'ops'):
            return
        retried=0
        if job_id is not None:
            row = await self.community_store.get_scheduled_job(job_id)
            if row and await self.community_store.reschedule_scheduled_job(job_id, run_at=_format_dt(_utc_now())):
                retried = 1
        else:
            rows = await self.community_store.list_scheduled_jobs(statuses=('failed','retry','dead_letter','cancelled'), limit=max(limit * 5, 50))
            threshold = _utc_now().timestamp() - (self.settings.job_dlq_after_hours * 3600)
            for row in rows:
                updated_dt = _parse_datetime(str(row.get('updated_at') or ''))
                if updated_dt and updated_dt.timestamp() > threshold:
                    continue
                if await self.community_store.reschedule_scheduled_job(int(row.get('id') or 0), run_at=_format_dt(_utc_now())):
                    retried += 1
                if retried >= limit:
                    break
        await self.bot.record_audit(action='job_dead_letter_requeue', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'job_id': job_id, 'retried': retried})
        await interaction.response.send_message(f'Повторно поставлено в очередь заданий: {retried}.', ephemeral=True)


    async def _create_forum_topic(
        self,
        *,
        interaction: discord.Interaction,
        forum_channel_id: int | None,
        title: str,
        body: str,
        audit_action: str,
        bridge_event_kind: str,
        topic_kind: str,
        extra_payload: dict[str, Any] | None = None,
        attachments: list[discord.Attachment] | None = None,
    ) -> tuple[discord.Thread | None, str | None]:
        attachments = attachments or []
        maintenance = await self.bot.get_maintenance_mode()
        if maintenance.get('enabled') and not self.bot.is_staff(interaction.user):
            return None, str(maintenance.get('message') or 'Сейчас включён режим технических работ.')
        forum = self.bot._get_forum_channel(forum_channel_id)
        if forum is None:
            return None, 'Нужный forum-канал не настроен или недоступен.'
        attachment_error = self._validate_attachments(topic_kind, attachments)
        if attachment_error:
            return None, attachment_error
        duplicate = await self.community_store.find_duplicate_forum_topic(
            guild_id=str(interaction.guild_id or ''),
            topic_kind=topic_kind,
            owner_user_id=str(interaction.user.id),
            title=title,
            target_user_id=str((extra_payload or {}).get('target_user_id') or '' or None),
            limit=50,
        )
        if duplicate is not None:
            return None, f"Похожая активная тема уже существует: <#{duplicate.get('thread_id')}>"
        attachment_text, attachment_meta = self._attachments_section(attachments)
        extra_payload = dict(extra_payload or {})
        extra_payload.update(attachment_meta)
        extra_payload['created_via'] = 'discord'
        body_rendered = self._forum_template(topic_kind, title, body, {**extra_payload, 'attachments_hint': attachment_text.strip() or 'Без вложений.'})
        content = (body_rendered + attachment_text).strip()[:4000]
        tag_names = self.bot._forum_tag_names_for_kind(topic_kind, 'open')
        tags = await self.bot._ensure_forum_tags(forum, tag_names)
        try:
            thread = await forum.create_thread(name=title[:100], content=content, applied_tags=tags[:5] if tags else None)
        except Exception as exc:
            LOGGER.exception('Failed to create forum topic in %s', forum.id)
            return None, f'Не удалось создать тему: {exc}'
        auto_close_after_seconds = int(self._forum_policy(topic_kind).get('auto_close_after_seconds') or self.settings.forum_auto_close_inactive_hours * 3600)
        metadata = {'staff_owner_user_id': '', **extra_payload}
        await self.community_store.register_forum_topic(
            thread_id=str(thread.id),
            guild_id=str(interaction.guild_id or ''),
            forum_channel_id=str(forum.id),
            topic_kind=topic_kind,
            owner_user_id=str(interaction.user.id),
            title=title[:250],
            tags=[tag.name for tag in tags[:5]] if tags else [self.settings.forum_tag_status_open_name],
            metadata=metadata,
            auto_close_after_seconds=auto_close_after_seconds,
        )
        await self.bot.record_audit(action=audit_action, actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'thread_id': str(thread.id), 'title': title, 'topic_kind': topic_kind, **extra_payload})
        bridge_payload = {'thread_id': str(thread.id), 'title': title, 'body': body[:2000], 'topic_kind': topic_kind, 'guild_id': str(interaction.guild_id or ''), 'actor_user_id': str(interaction.user.id), **extra_payload}
        await self.bot.queue_bridge_event(bridge_event_kind, bridge_payload)
        return thread, None

    @app_commands.command(name="chronicle_entry", description="Опубликовать запись в хронику мира NeverMine")
    @app_commands.default_permissions(manage_messages=True)
    async def chronicle_entry(self, interaction: discord.Interaction, title: str, body: str) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        if interaction.guild is None:
            await interaction.response.send_message('Команду нужно запускать на сервере.', ephemeral=True)
            return
        thread = await _create_external_forum_topic(self.bot, interaction.guild, topic_kind='chronicle', title=title, body=body, actor_user_id=str(interaction.user.id), actor_name=str(interaction.user), metadata={'source': 'discord-content'})
        if thread is None:
            await interaction.response.send_message('Не удалось опубликовать запись в хронику.', ephemeral=True)
            return
        await self.bot.record_audit(action='chronicle_entry_created', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'thread_id': str(thread.id), 'title': title})
        await self.bot.queue_bridge_event('community.chronicle.created', {'thread_id': str(thread.id), 'title': title, 'body': body[:2000], 'actor_user_id': str(interaction.user.id)})
        await interaction.response.send_message(f'Запись опубликована: {thread.mention}', ephemeral=True)

    @app_commands.command(name="lore_discussion", description="Создать тему для обсуждения лора NeverMine")
    async def lore_discussion(self, interaction: discord.Interaction, title: str, body: str, attachment: discord.Attachment | None = None, attachment_2: discord.Attachment | None = None, attachment_3: discord.Attachment | None = None) -> None:
        if not await self._enforce_command_cooldown(interaction, 'lore_discussion'):
            return
        if interaction.guild is None:
            await interaction.response.send_message('Команду нужно запускать на сервере.', ephemeral=True)
            return
        forum = _resolve_forum_for_topic(self.bot, interaction.guild, 'lore_discussion')
        forum_channel_id = getattr(forum, 'id', None)
        thread, error = await self._create_forum_topic(
            interaction=interaction,
            forum_channel_id=int(forum_channel_id) if forum_channel_id else None,
            title=title,
            body=body,
            audit_action='lore_discussion_created',
            bridge_event_kind='community.lore_discussion.created',
            topic_kind='lore_discussion',
            attachments=self._iter_attachments(attachment, attachment_2, attachment_3),
        )
        if error:
            await interaction.response.send_message(error, ephemeral=True)
            return
        await interaction.response.send_message(f'Тема обсуждения лора создана: {thread.mention if thread else "готово"}', ephemeral=True)

    @app_commands.command(name="world_signal_publish", description="Опубликовать world signal в read-only канале мира")
    @app_commands.default_permissions(manage_messages=True)
    async def world_signal_publish(self, interaction: discord.Interaction, title: str, body: str) -> None:
        if not await self._require_scope(interaction, 'content'):
            return
        if interaction.guild is None:
            await interaction.response.send_message('Команду нужно запускать на сервере.', ephemeral=True)
            return
        channel = _resolve_layout_channel(self.bot, interaction.guild, 'world_signals')
        if channel is None or not isinstance(channel, discord.abc.Messageable):
            await interaction.response.send_message('Канал world-signals не найден.', ephemeral=True)
            return
        embed = discord.Embed(title=title[:256], description=body[:4000], color=EMBED_COLOR)
        embed.set_footer(text=f'NeverMine world signal • {interaction.user.display_name}')
        message = await channel.send(embed=embed)
        await self.bot.record_audit(action='world_signal_published', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'message_id': getattr(message, 'id', None), 'title': title})
        await self.bot.queue_bridge_event('community.world_signal.created', {'message_id': str(getattr(message, 'id', '')), 'title': title, 'body': body[:2000], 'actor_user_id': str(interaction.user.id)})
        await interaction.response.send_message('World signal опубликован.', ephemeral=True)

    @app_commands.command(name="discord_scheduled_event", description="Создать нативное запланированное событие Discord")
    @app_commands.default_permissions(manage_events=True)
    async def discord_scheduled_event(self, interaction: discord.Interaction, name: str, starts_at: str, description: str | None = None) -> None:
        if not await self._require_scope(interaction, 'events'):
            return
        if interaction.guild is None:
            await interaction.response.send_message('Команду нужно запускать на сервере.', ephemeral=True)
            return
        dt = _parse_datetime(starts_at)
        if dt is None:
            await interaction.response.send_message('Укажи время в ISO-формате.', ephemeral=True)
            return
        location = f"Discord stage #{self.settings.discord_stage_channel_id}" if self.settings.discord_stage_channel_id else 'NeverMine Discord'
        try:
            event = await interaction.guild.create_scheduled_event(name=name[:100], start_time=dt, end_time=dt, description=(description or name)[:1000], entity_type=discord.EntityType.external, privacy_level=discord.PrivacyLevel.guild_only, location=location)
        except Exception as exc:
            await interaction.response.send_message(f'Не удалось создать Discord Scheduled Event: {exc}', ephemeral=True)
            return
        await self.bot.record_audit(action='discord_scheduled_event_created', actor_user_id=interaction.user.id, target_user_id=None, status='success', payload={'name': name, 'event_id': str(event.id), 'starts_at': _format_dt(dt)})
        await interaction.response.send_message(f'Событие Discord создано: {event.url}', ephemeral=True)



GroupedCommandAliases = build_grouped_command_aliases(CommunityCommands)


class NMDiscordBot(commands.Bot):
    def __init__(self, settings: Settings, storage: StorageManager) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = settings.bot_use_prefix_commands
        super().__init__(command_prefix=settings.command_prefix, intents=intents, help_command=None)
        self.settings = settings
        self.version = __version__
        self.storage = storage
        self.community_store = CommunityStore(storage, code_version=__version__)
        self.api = NeverMineApiClient(
            base_url=settings.nevermine_api_base_url,
            api_token=settings.nevermine_api_token,
            status_endpoint=settings.nevermine_status_endpoint,
            players_endpoint=settings.nevermine_players_endpoint,
            announcements_endpoint=settings.nevermine_announcements_endpoint,
            events_endpoint=settings.nevermine_events_endpoint,
            verify_start_endpoint=settings.nevermine_verify_start_endpoint,
            verify_complete_endpoint=settings.nevermine_verify_complete_endpoint,
            link_status_endpoint=settings.nevermine_link_status_endpoint,
            link_unlink_endpoint=settings.nevermine_link_unlink_endpoint,
            timeout=settings.nevermine_request_timeout,
            retries=settings.nevermine_request_retries,
            retry_backoff_seconds=settings.nevermine_request_retry_backoff_seconds,
            retry_backoff_max_seconds=settings.nevermine_request_retry_backoff_max_seconds,
        )
        self.relay_task: asyncio.Task[None] | None = None
        self.cleanup_task: asyncio.Task[None] | None = None
        self.external_sync_task: asyncio.Task[None] | None = None
        self.scheduler_task: asyncio.Task[None] | None = None
        self.approval_expiry_task: asyncio.Task[None] | None = None
        self.rules_reacceptance_task: asyncio.Task[None] | None = None
        self.drift_monitor_task: asyncio.Task[None] | None = None
        self.metrics_counters: Counter[str] = Counter()
        self.persistent_metrics_counters: dict[str, int] = {}
        self.bridge_destination_state_snapshot: dict[str, dict[str, Any]] = {}
        self.http_ingress = BridgeIngressServer(
            bot=self,
            host=settings.ingress_host,
            port=settings.ingress_port,
            enabled=settings.ingress_enabled,
            bearer_token=settings.ingress_bearer_token,
            hmac_secret=settings.ingress_hmac_secret,
            strict_auth=settings.ingress_strict_auth,
        )
        self.runtime_owner = f"discord-runtime:{id(self)}"
        self.started_at = _utc_now()
        self.layout_alias_bindings_cache: dict[tuple[str, str], str] = {}



def load_content_snapshot(self) -> dict[str, Any]:
    return load_content_snapshot_from_path(self.settings.discord_content_file_path)

async def get_runtime_marker(self, key: str) -> Any | None:
    return await self.storage.database.get_key_value(f'runtime_marker:{key}')

async def set_runtime_marker(self, key: str, value: Any) -> None:
    await self.storage.database.set_key_value(f'runtime_marker:{key}', value)

async def refresh_layout_alias_cache(self, guild_id: str | None = None) -> dict[tuple[str, str], str]:
    guild_ref = str(guild_id or self.settings.discord_guild_id or '')
    cache: dict[tuple[str, str], str] = {}
    if guild_ref:
        for row in await self.community_store.list_layout_alias_bindings(guild_id=guild_ref):
            alias = str(row.get('alias') or '').strip().lower()
            resource_type = str(row.get('resource_type') or 'channel').strip().lower()
            discord_id = str(row.get('discord_id') or '').strip()
            if alias and resource_type and discord_id:
                cache[(resource_type, alias)] = discord_id
    self.layout_alias_bindings_cache = cache
    return cache

async def remember_layout_alias_binding(self, guild_id: str, *, alias: str, resource_type: str, discord_id: int | str, metadata: dict[str, Any] | None = None) -> None:
    alias_norm = str(alias or '').strip().lower()
    resource_norm = str(resource_type or 'channel').strip().lower()
    discord_ref = str(discord_id or '').strip()
    if not alias_norm or not discord_ref:
        return
    await self.community_store.upsert_layout_alias_binding(guild_id=str(guild_id), alias=alias_norm, resource_type=resource_norm, discord_id=discord_ref, metadata=metadata or {})
    cache = getattr(self, 'layout_alias_bindings_cache', {}) or {}
    cache[(resource_norm, alias_norm)] = discord_ref
    self.layout_alias_bindings_cache = cache

async def export_operational_state(self, *, guild_id: str | None) -> dict[str, Any]:
    guild_ref = str(guild_id or self.settings.discord_guild_id or '')
    runtime_markers = {}
    for key in ('last_backup', 'last_runtime_drift_warning', 'runtime_drift_active'):
        try:
            runtime_markers[key] = await self.get_runtime_marker(key)
        except Exception:
            runtime_markers[key] = None
    return {
        'maintenance_mode': await self.get_maintenance_mode(),
        'runtime_forum_policy_overrides': getattr(self, 'runtime_forum_policy_overrides', {}),
        'panel_registry': await self.community_store.list_panel_bindings(guild_id=guild_ref) if guild_ref else [],
        'layout_alias_bindings': await self.community_store.list_layout_alias_bindings(guild_id=guild_ref) if guild_ref else [],
        'topics': await self.community_store.list_forum_topics(limit=250),
        'scheduled_jobs': await self.community_store.list_scheduled_jobs(statuses=('pending','retry','failed','dead_letter','sent','cancelled'), limit=250),
        'failed_bridge_events': await self.community_store.list_failed_external_sync_events(limit=250),
        'bridge_destination_state': await self.community_store.list_bridge_destination_states(),
        'schema_meta': await self.community_store.list_schema_meta(),
        'schema_meta_ledger': await self.community_store.list_schema_meta_ledger(limit=100),
        'panel_drift': await self.community_store.list_recent_panel_drift(guild_id=guild_ref, limit=50),
        'runtime_markers': runtime_markers,
        'bridge_comment_mirror': await self.community_store.list_bridge_comment_mirrors(limit=250),
        'external_discussion_mirror': await self.community_store.list_external_discussion_mirrors(limit=250),
        'external_content_mirror': await self.community_store.list_external_content_mirrors(limit=250),
        'content_pack_meta': (self.load_content_snapshot() or {}).get('meta', {}),
        'layout_spec_meta': (load_server_layout(ensure_server_layout_file()) or {}).get('meta', {}),
        'runtime_markers_snapshot': collect_runtime_markers(self),
        'build_metadata': {'version': self.version, 'content_schema_version': self.settings.content_schema_version_required, 'content_path': str(self.settings.discord_content_file_path)},
    }

async def capture_operational_backup(self, *, reason: str, actor_user_id: int | None = None, guild_id: int | None = None) -> Path | None:
    if not self.settings.backup_on_critical_changes:
        return None
    try:
        self.settings.backup_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        LOGGER.warning("BACKUP_DIR is not writable/creatable: %s; skipping operational backup: %s", self.settings.backup_dir, exc)
        return None
    snapshot = await self.export_operational_state(guild_id=str(guild_id or self.settings.discord_guild_id or ''))
    payload = {
        'version': self.version,
        'reason': reason,
        'actor_user_id': actor_user_id,
        'created_at': _format_dt(_utc_now()),
        'snapshot': snapshot,
    }
    filename = f"nmdiscordbot-backup-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{reason.replace(' ','-')[:40]}.json"
    path = self.settings.backup_dir / filename
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    except OSError as exc:
        LOGGER.warning("Failed to write operational backup to %s; continuing without backup: %s", path, exc)
        return None
    await self.set_runtime_marker('last_backup', {'path': str(path), 'reason': reason, 'created_at': payload['created_at']})
    return path

async def get_maintenance_mode(self) -> dict[str, Any]:
    state = getattr(self, 'maintenance_mode', None)
    if isinstance(state, dict):
        return state
    value = await self.storage.database.get_key_value('maintenance_mode')
    if isinstance(value, dict):
        self.maintenance_mode = value
        return value
    self.maintenance_mode = {'enabled': self.settings.maintenance_mode_default, 'message': self.settings.maintenance_mode_message}
    return self.maintenance_mode

async def build_thread_transcript(self, thread: discord.Thread, mode: str = 'staff') -> tuple[str, bytes]:
    cog = self.get_cog('CommunityCommands')
    if cog is None:
        return f"thread-{thread.id}.txt", f"Тема {thread.id}".encode('utf-8')
    return await cog._build_thread_transcript(thread, mode=mode)

async def notify_topic_owner_change(self, *, thread: discord.Thread, new_owner_user_id: str, actor_name: str) -> None:
    with contextlib.suppress(Exception):
        await thread.send(f"<@{new_owner_user_id}> назначен(а) ответственным за тему. Назначил: {actor_name}.")

async def apply_thread_status(self, *, thread: discord.Thread, topic_kind: str, status: str, metadata: dict[str, Any] | None = None) -> list[str]:
    metadata = metadata or {}
    cog = self.get_cog('CommunityCommands')
    if cog is not None:
        return await cog._sync_topic_presentation(thread, topic_kind=topic_kind, status=status, metadata=metadata, archive_override=(status in {'resolved','closed'}))
    await self.community_store.update_forum_topic_state(thread_id=str(thread.id), status=status, tags=[], metadata=metadata, closed=status in {'resolved','closed'})
    return []


def _normalized_member_role_names(self, member: discord.Member) -> set[str]:
    return {str(role.name).strip().lower() for role in member.roles if getattr(role, 'name', None)}

def has_staff_scope(self, user: discord.abc.User, scope: str | None = None) -> bool:
    if user.id in self.settings.admin_user_ids:
        return True
    if not isinstance(user, discord.Member):
        return False
    if user.guild_permissions.administrator:
        return True
    role_ids = {role.id for role in user.roles}
    role_names = self._normalized_member_role_names(user)
    generic_staff = bool(role_ids.intersection(self.settings.staff_role_ids))
    scope = (scope or 'staff').strip().lower()
    scope_role_ids = {
        'staff': set(self.settings.staff_role_ids),
        'moderation': set(self.settings.moderation_role_ids),
        'support': set(self.settings.support_role_ids),
        'content': set(self.settings.content_role_ids),
        'events': set(self.settings.event_role_ids),
        'community': set(self.settings.community_manager_role_ids),
        'ops': set(self.settings.moderation_role_ids) | set(self.settings.community_manager_role_ids) | set(self.settings.content_role_ids) | set(self.settings.event_role_ids),
    }
    keyword_map = {
        'staff': {'founder', 'administrator', 'lead moderator', 'moderator', 'community manager', 'support', 'lore / content', 'event team', 'основатель', 'администратор', 'старший модератор', 'модератор', 'поддержка', 'комьюнити менеджер'},
        'moderation': {'founder', 'administrator', 'lead moderator', 'moderator', 'основатель', 'администратор', 'старший модератор', 'модератор'},
        'support': {'founder', 'administrator', 'lead moderator', 'moderator', 'support', 'основатель', 'администратор', 'старший модератор', 'модератор', 'поддержка'},
        'content': {'founder', 'administrator', 'community manager', 'lore / content', 'deeplayer team', 'cajeer team', 'основатель', 'администратор', 'комьюнити менеджер', 'контент'},
        'events': {'founder', 'administrator', 'community manager', 'event team', 'основатель', 'администратор', 'комьюнити менеджер', 'event team'},
        'community': {'founder', 'administrator', 'community manager', 'основатель', 'администратор', 'комьюнити менеджер'},
        'ops': {'founder', 'administrator', 'community manager', 'deeplayer team', 'cajeer team', 'основатель', 'администратор', 'комьюнити менеджер'},
    }
    dynamic_scope_roles = set()
    if getattr(self.settings, 'staff_scope_role_map', None):
        for key in self.settings.staff_scope_role_map.get(scope, tuple()):
            if str(key).isdigit():
                dynamic_scope_roles.add(int(key))
    if dynamic_scope_roles and role_ids.intersection(dynamic_scope_roles):
        return True
    if scope_role_ids.get(scope) and role_ids.intersection(scope_role_ids[scope]):
        return True
    if any(name in keyword_map.get(scope, set()) for name in role_names):
        return True
    return generic_staff

def is_staff(self, user: discord.abc.User, scope: str | None = None) -> bool:
    return self.has_staff_scope(user, scope)



def _top_level_slash_command_count(self) -> int:
    return len([command for command in self.tree.get_commands() if isinstance(command, (app_commands.Command, app_commands.Group))])


def _grouped_alias_root_count(self) -> int:
    return len([value for value in vars(GroupedCommandAliases).values() if isinstance(value, app_commands.Group)])


def _remove_legacy_flat_staff_commands(self, *, reason: str, stop_at_limit: bool = False, target_count: int | None = None) -> None:
    if not hasattr(self, '_removed_flat_aliases'):
        self._removed_flat_aliases = []
    before = self._top_level_slash_command_count()
    effective_target = target_count if target_count is not None else (DISCORD_TOP_LEVEL_SLASH_COMMAND_LIMIT if stop_at_limit else None)
    for command_name in LEGACY_FLAT_STAFF_COMMANDS:
        if effective_target is not None and self._top_level_slash_command_count() <= effective_target:
            break
        with contextlib.suppress(Exception):
            removed = self.tree.remove_command(command_name)
            if removed is not None and command_name not in self._removed_flat_aliases:
                self._removed_flat_aliases.append(command_name)
    after = self._top_level_slash_command_count()
    if after != before:
        LOGGER.warning(
            "Pruned %s legacy flat slash-command aliases (%s -> %s top-level commands): %s",
            before - after,
            before,
            after,
            reason,
        )


def _apply_command_surface_mode(self) -> None:
    mode = str(getattr(self.settings, 'command_surface_mode', 'compat') or 'compat').strip().lower()
    if not hasattr(self, '_removed_flat_aliases'):
        self._removed_flat_aliases = []
    self._missing_permission_gates = []
    if mode == 'grouped-only':
        self._remove_legacy_flat_staff_commands(reason='COMMAND_SURFACE_MODE=grouped-only')
    elif mode == 'legacy-only':
        # Grouped aliases are not added in setup_hook when legacy-only is selected.
        self._removed_flat_aliases = []

    command_count = self._top_level_slash_command_count()
    if command_count > DISCORD_TOP_LEVEL_SLASH_COMMAND_LIMIT:
        self._remove_legacy_flat_staff_commands(
            reason=f'Discord top-level slash-command limit exceeded ({command_count}/{DISCORD_TOP_LEVEL_SLASH_COMMAND_LIMIT})',
            stop_at_limit=True,
        )

    for command in self.tree.get_commands():
        if getattr(command, 'name', '') in LEGACY_FLAT_STAFF_COMMANDS and getattr(command, 'default_permissions', None) is None:
            self._missing_permission_gates.append(str(command.name))

async def setup_hook(self) -> None:
    await self.storage.connect()
    await self.api.open()
    await self.community_store.ensure_schema()
    await self.add_cog(CommunityCommands(self))
    self._removed_flat_aliases = []
    if self.settings.command_surface_mode != 'legacy-only':
        projected_count = self._top_level_slash_command_count() + self._grouped_alias_root_count()
        if self.settings.command_surface_mode == 'grouped-only':
            self._remove_legacy_flat_staff_commands(reason='COMMAND_SURFACE_MODE=grouped-only before grouped aliases registration')
        elif projected_count > DISCORD_TOP_LEVEL_SLASH_COMMAND_LIMIT:
            self._remove_legacy_flat_staff_commands(
                reason=f'projected command surface would exceed Discord limit ({projected_count}/{DISCORD_TOP_LEVEL_SLASH_COMMAND_LIMIT}) before grouped aliases registration',
                stop_at_limit=True,
                target_count=max(0, DISCORD_TOP_LEVEL_SLASH_COMMAND_LIMIT - self._grouped_alias_root_count()),
            )
        await self.add_cog(GroupedCommandAliases(self))
    self._apply_command_surface_mode()
    self._content_snapshot = self.load_content_snapshot()
    await load_persistent_runtime_metrics(self)
    maintenance = await self.storage.database.get_key_value('maintenance_mode')
    self.maintenance_mode = maintenance if isinstance(maintenance, dict) else {'enabled': self.settings.maintenance_mode_default, 'message': self.settings.maintenance_mode_message}
    runtime_policies = await self.storage.database.get_key_value('runtime_forum_policy_overrides')
    self.runtime_forum_policy_overrides = runtime_policies if isinstance(runtime_policies, dict) else dict(self.settings.forum_policy_overrides or {})
    await self.refresh_layout_alias_cache(str(self.settings.discord_guild_id or ''))
    self.add_view(OnboardingView(self))
    self.add_view(InterestRolesView(self))
    self.add_view(HelpPanelView(self))
    self.add_view(TopicActionsView(self, 0))
    if self.settings.bot_use_prefix_commands:
        self.add_command(prefix_ping)
    if self.http_ingress.enabled:
        await self.http_ingress.start()
    if not self.settings.discord_sync_commands_on_start:
        LOGGER.info("Skipping slash-command sync on startup (DISCORD_SYNC_COMMANDS_ON_START=false)")
        return
    if self.settings.discord_guild_id:
        guild = discord.Object(id=self.settings.discord_guild_id)
        self.tree.copy_global_to(guild=guild)
        synced = await self.tree.sync(guild=guild)
        LOGGER.info("Synced %s guild commands to guild %s", len(synced), self.settings.discord_guild_id)
    else:
        synced = await self.tree.sync()
        LOGGER.info("Synced %s global commands", len(synced))

async def on_ready(self) -> None:
    if self.user is None:
        return
    ensure_content_layout(self.settings)
    await self.change_presence(activity=discord.Game(name=self.settings.bot_presence_text))
    LOGGER.info("Bot logged in as %s (%s) version=%s", self.user.name, self.user.id, self.version)
    await self.set_runtime_marker('started_at', _format_dt(self.started_at))
    await self._validate_startup_resources()
    await self.refresh_layout_alias_cache(str(self.settings.discord_guild_id or ''))
    await self.refresh_layout_alias_cache(str(self.settings.discord_guild_id or ''))
    if self.storage.redis_degraded:
        await self.record_audit(
            action="redis_degraded_mode",
            actor_user_id=None,
            target_user_id=None,
            status="warning",
            payload={"error": self.storage.redis_last_error or "redis unavailable", "allow_degraded_without_redis": self.settings.allow_degraded_without_redis},
        )
    if self.cleanup_task is None:
        self.cleanup_task = asyncio.create_task(self._cleanup_loop(), name="cleanup-loop")
        LOGGER.info("Cleanup loop started")
    if self.settings.relay_enabled and self.api.configured() and self.relay_task is None:
        self.relay_task = asyncio.create_task(self._relay_loop(), name="relay-loop")
        LOGGER.info("Relay loop started")
    if not self.settings.recovery_mode_default and self.external_sync_task is None:
        self.external_sync_task = asyncio.create_task(self._external_sync_loop(), name="external-sync-loop")
        LOGGER.info("External sync loop started")
    if not self.settings.recovery_mode_default and self.scheduler_task is None:
        self.scheduler_task = asyncio.create_task(self._scheduler_loop(), name="scheduler-loop")
        LOGGER.info("Scheduler loop started")
    if not self.settings.recovery_mode_default and self.approval_expiry_task is None:
        self.approval_expiry_task = asyncio.create_task(self._approval_expiry_loop(), name='approval-expiry-loop')
        LOGGER.info('Approval expiry loop started')
    if not self.settings.recovery_mode_default and self.rules_reacceptance_task is None:
        self.rules_reacceptance_task = asyncio.create_task(self._rules_reacceptance_loop(), name='rules-reacceptance-loop')
        LOGGER.info('Rules reacceptance loop started')
    if not self.settings.recovery_mode_default and self.settings.panel_auto_reconcile_on_ready and self.settings.discord_guild_id:
        await self._reconcile_panels(self.settings.discord_guild_id)
    if self.settings.drift_monitor_enabled and self.drift_monitor_task is None:
        self.drift_monitor_task = asyncio.create_task(self._runtime_drift_loop(), name='runtime-drift-loop')
        LOGGER.info('Runtime drift monitor started')

async def close(self) -> None:
    if self.cleanup_task is not None:
        self.cleanup_task.cancel()
        try:
            await self.cleanup_task
        except asyncio.CancelledError:
            pass
    if self.relay_task is not None:
        self.relay_task.cancel()
        try:
            await self.relay_task
        except asyncio.CancelledError:
            pass
    if self.external_sync_task is not None:
        self.external_sync_task.cancel()
        try:
            await self.external_sync_task
        except asyncio.CancelledError:
            pass
    if self.scheduler_task is not None:
        self.scheduler_task.cancel()
        try:
            await self.scheduler_task
        except asyncio.CancelledError:
            pass
    if self.approval_expiry_task is not None:
        self.approval_expiry_task.cancel()
        try:
            await self.approval_expiry_task
        except asyncio.CancelledError:
            pass
    if self.rules_reacceptance_task is not None:
        self.rules_reacceptance_task.cancel()
        try:
            await self.rules_reacceptance_task
        except asyncio.CancelledError:
            pass
    if self.drift_monitor_task is not None:
        self.drift_monitor_task.cancel()
        try:
            await self.drift_monitor_task
        except asyncio.CancelledError:
            pass
    await self.http_ingress.stop()
    await self.api.close()
    await self.storage.close()
    # close() is bound to NMDiscordBot dynamically by bind_bot_extensions().
    # A top-level function has no compiler-created __class__ cell, so
    # zero-argument super() fails at runtime. Call the base class explicitly.
    await commands.Bot.close(self)

async def sync_verified_role(self, user: discord.abc.User, *, linked: bool) -> None:
    if self.settings.verified_role_id is None or not isinstance(user, discord.Member):
        return
    role = user.guild.get_role(self.settings.verified_role_id)
    bot_member = user.guild.me
    if role is None or bot_member is None:
        return
    if not bot_member.guild_permissions.manage_roles:
        LOGGER.warning("Cannot sync verified role: bot lacks Manage Roles permission")
        return
    if bot_member.top_role.position <= role.position:
        LOGGER.warning("Cannot sync verified role: bot role hierarchy is not above VERIFIED_ROLE_ID=%s", role.id)
        return
    has_role = any(existing.id == role.id for existing in user.roles)
    if linked and not has_role:
        await user.add_roles(role, reason="NeverMine: привязка аккаунта")
    elif not linked and has_role:
        await user.remove_roles(role, reason="NeverMine: отвязка аккаунта")

async def record_audit(self, *, action: str, actor_user_id: int | None, target_user_id: int | None, status: str, payload: dict[str, Any]) -> None:
    await increment_runtime_metric(self, f'audit_{action}', 1)
    safe_payload = _prepare_audit_payload(self.settings, payload)
    await self.storage.append_audit_log(
        action=action,
        actor_user_id=actor_user_id,
        target_user_id=target_user_id,
        status=status,
        payload=safe_payload,
    )
    category = _audit_category(action)
    channel_ids = self._get_audit_channel_ids(category)
    if not channel_ids:
        return
    color = AUDIT_COLOR if status == "success" else (0xF1C40F if status in {"warning", "degraded"} else ERROR_COLOR)
    embed = discord.Embed(title=f"аудит / {audit_category_label(category)} • {action}", color=color)
    embed.add_field(name="статус", value=audit_status_label(status), inline=True)
    embed.add_field(name="инициатор", value=str(actor_user_id or "—"), inline=True)
    embed.add_field(name="цель", value=str(target_user_id or "—"), inline=True)
    embed.description = _preview_payload(safe_payload, max_length=self.settings.audit_relay_max_preview_length)
    sent_to: set[int] = set()
    for channel_id in channel_ids:
        if channel_id in sent_to:
            continue
        channel = self._get_message_channel(channel_id)
        if channel is None:
            continue
        mention = self._interest_ping_mentions('community.event.created')
        await channel.send(content=mention or None, embed=embed)
        sent_to.add(channel_id)


async def _ensure_forum_tags(self, forum: discord.ForumChannel, names: list[str]) -> list[discord.ForumTag]:
    existing = {tag.name.strip().lower(): tag for tag in forum.available_tags}
    created: list[discord.ForumTag] = []
    missing = [name for name in names if name and name.strip().lower() not in existing]
    for name in missing[:10]:
        try:
            await forum.create_tag(name=name[:20])
        except Exception:
            LOGGER.exception('Failed to create forum tag %s in forum %s', name, forum.id)
    refreshed = {tag.name.strip().lower(): tag for tag in forum.available_tags}
    return [refreshed[name.strip().lower()] for name in names if name and name.strip().lower() in refreshed]

def _forum_tag_names_for_kind(self, topic_kind: str, status: str) -> list[str]:
    kind_map = {
        'support': self.settings.forum_tag_support_name,
        'bug': self.settings.forum_tag_bug_name,
        'suggestion': self.settings.forum_tag_suggestion_name,
        'appeal': self.settings.forum_tag_appeal_name,
        'guild_recruitment': self.settings.forum_tag_guild_recruitment_name,
        'report': 'репорт',
        'chronicle': 'хроника',
        'lore_discussion': 'лор',
    }
    status_map = {
        'open': self.settings.forum_tag_status_open_name,
        'in_review': self.settings.forum_tag_status_in_review_name,
        'resolved': self.settings.forum_tag_status_resolved_name,
        'closed': self.settings.forum_tag_status_closed_name,
    }
    out: list[str] = []
    if kind_map.get(topic_kind):
        out.append(kind_map[topic_kind])
    layout = load_server_layout(ensure_server_layout_file())
    aliases = forum_aliases_by_kind(layout).get(str(topic_kind or '').strip().lower(), [])
    layout_tags = expected_forum_tags(layout)
    for alias in aliases:
        for tag in layout_tags.get(alias, []):
            if tag and tag.lower() not in {x.lower() for x in out}:
                out.append(tag)
    if status_map.get(status):
        out.append(status_map[status])
    return [x for x in out if x]

def _interest_ping_mentions(self, event_kind: str) -> str:
    mapping = getattr(self.settings, 'interest_role_ping_map', {}) or {}
    role_keys = mapping.get(event_kind, ()) or mapping.get('*', ())
    role_ids = self._interest_role_ids()
    mentions = []
    for key in role_keys:
        rid = role_ids.get(str(key).strip().lower())
        if rid:
            mentions.append(f'<@&{rid}>')
    return ' '.join(dict.fromkeys(mentions))

async def _subscription_event_mentions(self, guild: discord.Guild | None, *, event_kind: str) -> str:
    if guild is None:
        return ''
    interest_aliases = routed_interest_aliases(event_kind)
    role_ids_map = self._interest_role_ids()
    interest_role_ids = [role_ids_map.get(alias) for alias in interest_aliases if role_ids_map.get(alias)]
    rows = await self.community_store.list_matching_subscription_targets(platform='discord', event_kind=event_kind, interest_role_ids=[rid for rid in interest_role_ids if rid], limit=250)
    mentions: list[str] = []
    for row in rows:
        user_id = str(row.get('platform_user_id') or '').strip()
        if not user_id.isdigit():
            continue
        member = guild.get_member(int(user_id))
        if member is None:
            continue
        mentions.append(member.mention)
    return ' '.join(dict.fromkeys(mentions))

async def _sync_topic_presentation(self, thread: discord.Thread, *, topic_kind: str, status: str, metadata: dict[str, Any] | None = None, archive_override: bool = False) -> list[str]:
    metadata = dict(metadata or {})
    tags = self._forum_tag_names_for_kind(topic_kind, status)
    if isinstance(thread.parent, discord.ForumChannel):
        forum = thread.parent
        forum_tags = await self._ensure_forum_tags(forum, tags) if self.settings.forum_auto_create_tags else [tag for tag in forum.available_tags if tag.name in tags]
        with contextlib.suppress(Exception):
            await thread.edit(applied_tags=forum_tags[:5], archived=archive_override)
    title_prefix_map = {'open': 'Открыто', 'in_review': 'На рассмотрении', 'resolved': 'Решено', 'closed': 'Закрыто'}
    desired_prefix = title_prefix_map.get(status, '').strip()
    current_name = thread.name
    stripped = re.sub(r'^\[[^\]]+\]\s*', '', current_name).strip()
    new_name = f'[{desired_prefix}] {stripped}' if desired_prefix and self.settings.forum_status_source in {'title','hybrid'} else stripped
    if new_name != current_name:
        with contextlib.suppress(Exception):
            await thread.edit(name=new_name[:100])
    await self.community_store.update_forum_topic_state(thread_id=str(thread.id), status=status, tags=tags, metadata=metadata, closed=status in {'resolved','closed'})
    return tags

async def _validate_startup_resources(self) -> None:
    me = self.user
    if me is None:
        return
    guild: discord.Guild | None = None
    if self.settings.discord_guild_id:
        guild = self.get_guild(self.settings.discord_guild_id)
    elif len(self.guilds) == 1:
        guild = self.guilds[0]
    problems: list[str] = []

    channel_ids = [
        ("DISCORD_STATUS_CHANNEL_ID", self.settings.discord_status_channel_id),
        ("DISCORD_ANNOUNCEMENTS_CHANNEL_ID", self.settings.discord_announcements_channel_id),
        ("DISCORD_EVENTS_CHANNEL_ID", self.settings.discord_events_channel_id),
        ("DISCORD_AUDIT_CHANNEL_ID", self.settings.discord_audit_channel_id),
        ("DISCORD_SECURITY_AUDIT_CHANNEL_ID", self.settings.discord_security_audit_channel_id),
        ("DISCORD_BUSINESS_AUDIT_CHANNEL_ID", self.settings.discord_business_audit_channel_id),
        ("DISCORD_OPS_AUDIT_CHANNEL_ID", self.settings.discord_ops_audit_channel_id),
    ]
    for label, channel_id in channel_ids:
        if not channel_id:
            continue
        channel = self._get_message_channel(channel_id)
        if channel is None:
            problems.append(f"{label}: channel {channel_id} not found or not messageable")
            continue
        perms_for = getattr(channel, "permissions_for", None)
        if callable(perms_for) and me is not None:
            perms = perms_for(channel.guild.me if isinstance(channel, discord.abc.GuildChannel) else me)
            missing = []
            if hasattr(perms, "view_channel") and not perms.view_channel:
                missing.append("view_channel")
            if hasattr(perms, "send_messages") and not perms.send_messages:
                missing.append("send_messages")
            if hasattr(perms, "embed_links") and not perms.embed_links:
                missing.append("embed_links")
            if missing:
                problems.append(f"{label}: missing permissions {', '.join(missing)}")

    warnings: list[str] = []
    if self.intents.members:
        warnings.append("В коде включён members intent; проверь включение 'Server Members Intent' в Discord Developer Portal.")
    if self.settings.bot_use_prefix_commands:
        warnings.append("Включены prefix-команды; проверь включение 'Message Content Intent' в Discord Developer Portal.")
    if not self.settings.bot_use_prefix_commands and self.intents.message_content:
        problems.append("message_content intent is enabled while BOT_USE_PREFIX_COMMANDS=false")
    if self.http_ingress.enabled and not (self.settings.ingress_bearer_token or self.settings.ingress_hmac_secret):
        warnings.append("Ingress is enabled without bearer/HMAC authentication")
    if self._bridge_destinations() and not (self.settings.outbound_bearer_token or self.settings.outbound_hmac_secret):
        warnings.append("Bridge destinations are configured without outbound bearer/HMAC protection")

    if self.settings.verified_role_id is not None:
        if guild is None:
            problems.append("VERIFIED_ROLE_ID задан, но сервер не удалось определить для стартовой проверки")
        else:
            role = guild.get_role(self.settings.verified_role_id)
            if role is None:
                problems.append(f"VERIFIED_ROLE_ID: role {self.settings.verified_role_id} not found in guild {guild.id}")
            else:
                bot_member = guild.me
                if bot_member is None:
                    problems.append("VERIFIED_ROLE_ID configured, but guild.me is unavailable for startup validation")
                else:
                    if not bot_member.guild_permissions.manage_roles:
                        problems.append("VERIFIED_ROLE_ID configured, but bot lacks Manage Roles permission")
                    if bot_member.top_role.position <= role.position:
                        problems.append("VERIFIED_ROLE_ID configured, but bot role hierarchy is not above the target role")

    typed_channels: list[tuple[str, int | None, str]] = [
        ("DISCORD_START_HERE_CHANNEL_ID", self.settings.discord_start_here_channel_id, 'text'),
        ("DISCORD_RULES_CHANNEL_ID", self.settings.discord_rules_channel_id, 'text'),
        ("DISCORD_ROLES_AND_ACCESS_CHANNEL_ID", self.settings.discord_roles_channel_id, 'text'),
        ("DISCORD_FAQ_CHANNEL_ID", self.settings.discord_faq_channel_id, 'text'),
        ("DISCORD_DEVLOG_CHANNEL_ID", self.settings.discord_devlog_channel_id, 'text'),
        ("DISCORD_WORLD_SIGNALS_CHANNEL_ID", self.settings.discord_world_signals_channel_id, 'text'),
        ("DISCORD_REPORTS_CHANNEL_ID", self.settings.discord_reports_channel_id, 'text'),
        ("DISCORD_BOT_LOGS_CHANNEL_ID", self.settings.discord_bot_logs_channel_id, 'text'),
        ("DISCORD_STAGE_CHANNEL_ID", self.settings.discord_stage_channel_id, 'stage'),
        ("DISCORD_FORUM_SUGGESTIONS_CHANNEL_ID", self.settings.discord_forum_suggestions_channel_id, 'forum'),
        ("DISCORD_FORUM_BUG_REPORTS_CHANNEL_ID", self.settings.discord_forum_bug_reports_channel_id, 'forum'),
        ("DISCORD_FORUM_GUILD_RECRUITMENT_CHANNEL_ID", self.settings.discord_forum_guild_recruitment_channel_id, 'forum'),
        ("DISCORD_FORUM_HELP_CHANNEL_ID", self.settings.discord_forum_help_channel_id, 'forum'),
        ("DISCORD_FORUM_LAUNCHER_AND_TECH_CHANNEL_ID", self.settings.discord_forum_launcher_and_tech_channel_id, 'forum'),
        ("DISCORD_FORUM_ACCOUNT_HELP_CHANNEL_ID", self.settings.discord_forum_account_help_channel_id, 'forum'),
        ("DISCORD_FORUM_APPEALS_CHANNEL_ID", self.settings.discord_forum_appeals_channel_id, 'forum'),
    ]
    for label, channel_id, expected_kind in typed_channels:
        if not channel_id:
            continue
        channel = self.get_channel(channel_id)
        if channel is None:
            problems.append(f"{label}: channel {channel_id} not found")
            continue
        if expected_kind == 'forum' and not isinstance(channel, discord.ForumChannel):
            problems.append(f"{label}: channel {channel_id} is not a forum channel")
        elif expected_kind == 'stage' and not isinstance(channel, discord.StageChannel):
            problems.append(f"{label}: channel {channel_id} is not a stage channel")
        elif expected_kind == 'text' and not isinstance(channel, discord.abc.Messageable):
            problems.append(f"{label}: channel {channel_id} is not messageable")
        if expected_kind == 'forum' and isinstance(channel, discord.ForumChannel):
            expected_tags = self._forum_tag_names_for_kind('support', 'open') + self._forum_tag_names_for_kind('bug', 'resolved') + self._forum_tag_names_for_kind('guild_recruitment', 'closed')
            existing = {tag.name.strip().lower() for tag in channel.available_tags}
            missing = [name for name in {t for t in expected_tags if t} if name.strip().lower() not in existing]
            if missing:
                if self.settings.forum_auto_create_tags:
                    await self._ensure_forum_tags(channel, missing)
                    existing = {tag.name.strip().lower() for tag in channel.available_tags}
                    missing = [name for name in missing if name.strip().lower() not in existing]
                if missing:
                    warnings.append(f"{label}: missing forum tags {missing}")
            if isinstance(channel, discord.ForumChannel) and guild is not None and guild.me is not None:
                perms = channel.permissions_for(guild.me)
                needed = []
                for attr in ('view_channel', 'send_messages', 'create_public_threads', 'send_messages_in_threads', 'manage_threads'):
                    if hasattr(perms, attr) and not getattr(perms, attr):
                        needed.append(attr)
                if needed:
                    problems.append(f"{label}: недостаточно прав для forum automation: {', '.join(needed)}")
                if len(channel.available_tags) == 0:
                    warnings.append(f"{label}: у forum-канала не настроены теги; triage будет ограничен")
    if guild is not None:
        readonly_public_checks = [
            ("DISCORD_RULES_CHANNEL_ID", self.settings.discord_rules_channel_id),
            ("DISCORD_ROLES_AND_ACCESS_CHANNEL_ID", self.settings.discord_roles_channel_id),
            ("DISCORD_ANNOUNCEMENTS_CHANNEL_ID", self.settings.discord_announcements_channel_id),
            ("DISCORD_DEVLOG_CHANNEL_ID", self.settings.discord_devlog_channel_id),
            ("DISCORD_WORLD_SIGNALS_CHANNEL_ID", self.settings.discord_world_signals_channel_id),
        ]
        default_role = guild.default_role
        for label, channel_id in readonly_public_checks:
            channel = self.get_channel(channel_id) if channel_id else None
            if isinstance(channel, discord.abc.GuildChannel):
                perms = channel.permissions_for(default_role)
                if getattr(perms, 'send_messages', False):
                    warnings.append(f"{label}: public can still send messages; summary expects read-only")
        role_checks = [
            ("VISITOR_ROLE_ID", self.settings.visitor_role_id),
            ("MEMBER_ROLE_ID", self.settings.member_role_id),
            ("GUILD_LEADER_ROLE_ID", self.settings.guild_leader_role_id),
            ("INTEREST_ROLE_NEWS_ID", self.settings.interest_role_news_id),
            ("INTEREST_ROLE_LORE_ID", self.settings.interest_role_lore_id),
            ("INTEREST_ROLE_GAMEPLAY_ID", self.settings.interest_role_gameplay_id),
            ("INTEREST_ROLE_EVENTS_ID", self.settings.interest_role_events_id),
            ("INTEREST_ROLE_GUILDS_ID", self.settings.interest_role_guilds_id),
            ("INTEREST_ROLE_MEDIA_ID", self.settings.interest_role_media_id),
            ("INTEREST_ROLE_DEVLOGS_ID", self.settings.interest_role_devlogs_id),
        ]
        for label, role_id in role_checks:
            if role_id and guild.get_role(role_id) is None:
                problems.append(f"{label}: role {role_id} not found in guild {guild.id}")

        interest_role_ids = [rid for rid in self._interest_role_ids().values()]
        duplicates = {rid for rid in interest_role_ids if interest_role_ids.count(rid) > 1}
        if duplicates:
            problems.append(f"Interest roles contain duplicate IDs: {sorted(duplicates)}")
        protected_roles = {rid for rid in [self.settings.visitor_role_id, self.settings.member_role_id, self.settings.guild_leader_role_id, self.settings.verified_role_id] if rid}
        protected_roles.update(self.settings.staff_role_ids)
        for rid in interest_role_ids:
            if rid in protected_roles:
                problems.append(f"Interest role {rid} overlaps with protected/staff role configuration")
        bot_member = guild.me
        if bot_member is not None:
            for rid in interest_role_ids:
                role = guild.get_role(rid)
                if role and bot_member.top_role.position <= role.position:
                    problems.append(f"Interest role {rid} is not below the bot role in hierarchy")

    if self.storage.redis_degraded:
        warnings.append(f"Redis unavailable; running in degraded mode ({self.storage.redis_last_error or 'unknown error'})")

    for issue in validate_content_pack(self.settings):
        warnings.append(issue)
    expected_content = str(self.settings.discord_content_file_path)
    if expected_content != './templates/content.json' and expected_content != 'templates/content.json':
        warnings.append(f'Канонический путь content pack должен быть templates/content.json, сейчас задано: {expected_content}')

    if warnings:
        LOGGER.warning("Startup validation warnings: %s", '; '.join(warnings))

    if problems:
        message = "; ".join(problems)
        if self.settings.discord_startup_validation_strict:
            LOGGER.error("Startup validation failed: %s", message)
            await self.close()
            raise RuntimeError(message)
        LOGGER.warning("Startup validation warnings: %s", message)
    else:
        LOGGER.info("Startup resource validation passed")

async def _cleanup_loop(self) -> None:
    while True:
        try:
            lock_name = 'cleanup-loop'
            lock_token = await self.storage.acquire_lock(lock_name)
            runtime_lock = await self.community_store.acquire_runtime_lock(lock_name, self.runtime_owner, ttl_seconds=max(60, self.settings.cleanup_interval_seconds))
            if self.storage.cache.client is not None and not lock_token:
                await asyncio.sleep(min(self.settings.cleanup_interval_seconds, 300))
                continue
            if not runtime_lock:
                await self.storage.release_lock(lock_name, lock_token)
                await asyncio.sleep(min(self.settings.cleanup_interval_seconds, 300))
                continue
            try:
                deleted = await self.storage.purge_old_records(
                    audit_log_retention_days=self.settings.audit_log_retention_days,
                    verification_session_retention_days=self.settings.verification_session_retention_days,
                    relay_history_retention_days=self.settings.relay_history_retention_days,
                )
                optimize_actions: list[str] = []
                if deleted:
                    LOGGER.info("Cleanup loop removed stale rows: %s", deleted)
                    optimize_actions = await self.storage.optimize_sqlite(deleted_rows=deleted)
                    if optimize_actions:
                        LOGGER.info("SQLite maintenance executed: %s", optimize_actions)
                stale_topics = await self.community_store.list_stale_forum_topics()
                stale_closed = 0
                for topic in stale_topics:
                    thread = await self._resolve_thread(str(topic.get('thread_id') or ''))
                    if thread is not None and isinstance(thread, discord.Thread):
                        try:
                            await self._sync_topic_presentation(thread, topic_kind=str(topic.get('topic_kind') or 'support'), status='closed', metadata={'auto_closed': True}, archive_override=True)
                        except Exception:
                            LOGGER.exception('Failed to auto-close thread %s', topic.get('thread_id'))
                    await self.community_store.update_forum_topic_state(thread_id=str(topic.get('thread_id')), status='closed', tags=[self.settings.forum_tag_status_closed_name], metadata={'auto_closed': True}, closed=True)
                    stale_closed += 1
                await self._run_escalation_cycle()
                legacy_due = await self.community_store.list_legacy_layout_resources(guild_id=str(self.settings.discord_guild_id or ''), due_only=True, limit=10)
                if legacy_due:
                    await self._send_staff_notice('Legacy layout review: ' + '; '.join(legacy_review_summary(legacy_due)[:5]))
                    await self.set_runtime_marker('last_legacy_layout_review_notice', {'at': _format_dt(_utc_now()), 'count': len(legacy_due)})
                summary = {'executed_at': _format_dt(_utc_now()), 'deleted': deleted, 'stale_topics_closed': stale_closed, 'legacy_review_due': len(legacy_due)}
                if optimize_actions:
                    summary['sqlite_actions'] = optimize_actions
                await self.storage.database.set_key_value('cleanup_last_summary', summary)
                await self.set_runtime_marker('last_cleanup', summary)
            finally:
                await self.community_store.release_runtime_lock(lock_name, self.runtime_owner)
                await self.storage.release_lock(lock_name, lock_token)
            await asyncio.sleep(self.settings.cleanup_interval_seconds)
        except asyncio.CancelledError:
            LOGGER.info("Cleanup loop cancelled")
            raise
        except Exception:
            LOGGER.exception("Cleanup loop failed unexpectedly")
            await asyncio.sleep(min(self.settings.cleanup_interval_seconds, 300))

async def _relay_loop(self) -> None:
    failure_count = 0
    while True:
        try:
            await self._run_relay_iteration()
            failure_count = 0
            await asyncio.sleep(self.settings.relay_poll_interval_seconds)
        except asyncio.CancelledError:
            LOGGER.info("Relay loop cancelled")
            raise
        except Exception:
            failure_count += 1
            backoff = min(
                self.settings.nevermine_request_retry_backoff_seconds * (2 ** (failure_count - 1)),
                max(self.settings.relay_poll_interval_seconds, self.settings.nevermine_request_retry_backoff_max_seconds),
            )
            LOGGER.exception("Relay loop failed unexpectedly; retrying in %.1f sec", backoff)
            await asyncio.sleep(backoff)

async def _run_relay_iteration(self) -> None:
    lock_token = await self.storage.acquire_lock("relay-loop")
    runtime_lock = await self.community_store.acquire_runtime_lock("relay-loop", self.runtime_owner, ttl_seconds=max(30, self.settings.relay_poll_interval_seconds))
    if self.storage.cache.client is not None and not lock_token:
        LOGGER.debug("Skipping relay iteration: redis lock already held")
        return
    if not runtime_lock:
        LOGGER.debug("Skipping relay iteration: runtime lock already held")
        await self.storage.release_lock("relay-loop", lock_token)
        return
    try:
        if self.settings.relay_status_changes:
            await self._relay_status_change()
        if self.settings.relay_announcements:
            await self._relay_announcements()
        if self.settings.relay_events:
            await self._relay_events()
    finally:
        await self.community_store.release_runtime_lock("relay-loop", self.runtime_owner)
        await self.storage.release_lock("relay-loop", lock_token)

async def _relay_status_change(self) -> None:
    channel = self._get_message_channel(self.settings.discord_status_channel_id)
    if channel is None:
        return
    try:
        payload = await self.api.fetch_status()
    except NeverMineApiError:
        LOGGER.warning("Failed to poll status endpoint", exc_info=True)
        return
    online = bool(_pick(payload, ["online", "is_online", "server_online"], default=False))
    previous = await self.storage.get_status_online()
    if previous is None:
        await self.storage.set_status_online(online)
        return
    if previous == online:
        return
    await self.storage.set_status_online(online)
    embed = build_status_embed(self.settings.nevermine_server_name, payload)
    embed.title = f"{self.settings.nevermine_server_name} — статус изменился"
    await channel.send(embed=embed)
    await self.record_audit(
        action="relay_status_change",
        actor_user_id=None,
        target_user_id=None,
        status="success",
        payload={"online": online, "payload": payload},
    )
    await self.queue_bridge_event("community.status.changed", {"online": online, "payload": payload})
    LOGGER.info("Relayed status change: online=%s", online)

async def _relay_announcements(self) -> None:
    channel = self._get_message_channel(self.settings.discord_announcements_channel_id)
    if channel is None:
        return
    try:
        items = await self.api.fetch_announcements()
    except NeverMineApiError:
        LOGGER.warning("Failed to poll announcements endpoint", exc_info=True)
        return
    for item in items[:10]:
        item_id = str(item.get("id") or item.get("slug") or item.get("created_at") or item.get("title"))
        if await self.storage.is_known_relay_item("announcement", item_id):
            continue
        embed = discord.Embed(
            title=str(item.get("title") or "NeverMine — объявление"),
            description=str(item.get("text") or item.get("body") or item.get("description") or "—"),
            color=STAFF_COLOR,
        )
        if item.get("url"):
            embed.add_field(name="Ссылка", value=str(item.get("url")), inline=False)
        mentions = await self._subscription_event_mentions(self.get_guild(self.settings.discord_guild_id) if self.settings.discord_guild_id else None, event_kind='community.announcement.created')
        message = await channel.send(content=mentions or None, embed=embed)
        await self.community_store.upsert_external_content_mirror(source_platform='discord', content_kind='announcement', external_content_id=str(message.id), discord_channel_id=str(getattr(channel, 'id', '') or ''), discord_message_id=str(message.id), metadata={'origin': 'discord-command'})
        await self.storage.remember_relay_item("announcement", item_id, item)
        await self.record_audit(
            action="relay_announcement",
            actor_user_id=None,
            target_user_id=None,
            status="success",
            payload={"item_id": item_id},
        )
        await self.queue_bridge_event("community.announcement.created", {"item_id": item_id, "payload": item})
        LOGGER.info("Relayed announcement %s", item_id)

async def _relay_events(self) -> None:
    channel = self._get_message_channel(self.settings.discord_events_channel_id)
    if channel is None:
        return
    try:
        items = await self.api.fetch_events()
    except NeverMineApiError:
        LOGGER.warning("Failed to poll events endpoint", exc_info=True)
        return
    for item in items[:10]:
        item_id = str(item.get("id") or item.get("slug") or item.get("starts_at") or item.get("title"))
        if await self.storage.is_known_relay_item("event", item_id):
            continue
        embed = discord.Embed(
            title=str(item.get("title") or "NeverMine — событие"),
            description=str(item.get("description") or item.get("text") or "—"),
            color=EMBED_COLOR,
        )
        if item.get("starts_at"):
            embed.add_field(name="Старт", value=str(item.get("starts_at")), inline=True)
        if item.get("url"):
            embed.add_field(name="Ссылка", value=str(item.get("url")), inline=False)
        await channel.send(embed=embed)
        await self.storage.remember_relay_item("event", item_id, item)
        await self.record_audit(
            action="relay_event",
            actor_user_id=None,
            target_user_id=None,
            status="success",
            payload={"item_id": item_id},
        )
        await self.queue_bridge_event("community.event.created", {"item_id": item_id, "payload": item})
        LOGGER.info("Relayed event %s", item_id)


def build_metrics_text(self) -> str:
    return build_runtime_metrics_text(self)

async def _runtime_drift_loop(self) -> None:
    while True:
        try:
            await asyncio.sleep(self.settings.drift_monitor_interval_seconds)
            await self._runtime_drift_cycle()
        except asyncio.CancelledError:
            LOGGER.info('Runtime drift loop cancelled')
            raise
        except Exception:
            LOGGER.exception('Runtime drift loop failed unexpectedly')
            await asyncio.sleep(min(self.settings.drift_monitor_interval_seconds, 60))

async def _runtime_drift_cycle(self) -> None:
    warnings: list[str] = []
    lock_name = 'runtime-drift-loop'
    lock_token = await self.storage.acquire_lock(lock_name)
    runtime_lock = await self.community_store.acquire_runtime_lock(lock_name, self.runtime_owner, ttl_seconds=max(60, self.settings.drift_monitor_interval_seconds * 2))
    if self.storage.cache.client is not None and not lock_token:
        return
    if not runtime_lock:
        await self.storage.release_lock(lock_name, lock_token)
        return
    try:
        try:
            await self._validate_startup_resources()
            await self.refresh_layout_alias_cache(str(self.settings.discord_guild_id or ''))
        except Exception as exc:
            warnings.append(f'Стартовая проверка обнаружила drift: {exc}')
        warnings.extend(validate_content_pack(self.settings))
        warnings.extend(validate_server_layout(load_server_layout(ensure_server_layout_file())))
        try:
            self.bridge_destination_state_snapshot = {str(row.get('destination') or ''): row for row in await self.community_store.list_bridge_destination_states()}
        except Exception:
            self.bridge_destination_state_snapshot = {}
        guild = self.get_guild(self.settings.discord_guild_id) if self.settings.discord_guild_id else (self.guilds[0] if len(self.guilds) == 1 else None)
        if guild is not None:
            drift = await _collect_layout_drift(self, guild)
            warnings.extend(_summarize_layout_drift(drift, 'all'))
        active = await self.get_runtime_marker('runtime_drift_active') or {}
        if warnings:
            await self.set_runtime_marker('last_runtime_drift_warning', {'at': _format_dt(_utc_now()), 'issues': warnings[:20]})
            await self.record_audit(action='runtime_drift_warning', actor_user_id=None, target_user_id=None, status='warning', payload={'issues': warnings[:20]})
            await _post_runtime_drift_alert(self, warnings)
        elif isinstance(active, dict) and active.get('warnings'):
            await self.record_audit(action='runtime_drift_resolved', actor_user_id=None, target_user_id=None, status='success', payload={'resolved': True})
            await _post_runtime_drift_alert(self, [], resolved=True)
    finally:
        await self.community_store.release_runtime_lock(lock_name, self.runtime_owner)
        await self.storage.release_lock(lock_name, lock_token)

async def _external_sync_loop(self) -> None:
    while True:
        lock_name = 'external-sync-loop'
        acquired = await self.community_store.acquire_runtime_lock(lock_name, self.runtime_owner, ttl_seconds=max(30, self.settings.relay_poll_interval_seconds))
        try:
            if acquired:
                rows = await self.community_store.list_deliverable_external_sync_events(limit=self.settings.bridge_delivery_batch_size)
                for row in rows:
                    await self._deliver_external_sync_row(row)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception('External sync loop failed unexpectedly')
        finally:
            if acquired:
                await self.community_store.release_runtime_lock(lock_name, self.runtime_owner)
        await asyncio.sleep(max(5, min(self.settings.relay_poll_interval_seconds, 30)))

async def _deliver_external_sync_row(self, row: dict[str, Any]) -> None:
    destination = str(row.get('destination') or '')
    payload = row.get('payload_json') or {}
    event_id = int(row.get('id') or 0)
    attempt_count = int(row.get('attempt_count') or 0)
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {'raw': payload}
    state = await self.community_store.get_bridge_destination_state(destination) if destination else None
    open_until = _parse_datetime(str((state or {}).get('circuit_open_until') or ''))
    if open_until is not None and open_until > _utc_now():
        LOGGER.warning('Skipping external sync for %s: circuit open until %s', destination, open_until.isoformat())
        return
    backoff_seconds = _retry_backoff(attempt_count + 1, base_seconds=self.settings.bridge_retry_backoff_base_seconds, max_seconds=self.settings.bridge_retry_backoff_max_seconds)
    try:
        ok = await push_external_event(
            self.api.session,
            destination,
            payload,
            bearer_token=self.settings.outbound_bearer_token,
            hmac_secret=self.settings.outbound_hmac_secret,
            key_id=self.settings.outbound_key_id,
            timeout_seconds=self.settings.bridge_timeout_seconds,
        )
        if ok:
            await self.community_store.mark_external_sync_event(event_id, status='sent', error='')
            await self.community_store.update_bridge_destination_state(destination=destination, circuit_state='closed', consecutive_failures=0, last_error='', success=True, metadata={'event_id': event_id})
            await self.set_runtime_marker('last_bridge_success', {'destination': destination, 'event_id': event_id, 'at': _format_dt(_utc_now())})
        else:
            next_attempt = attempt_count + 1
            terminal = next_attempt >= self.settings.bridge_max_attempts
            failures = int((state or {}).get('consecutive_failures') or 0) + 1
            circuit_open = failures >= self.settings.bridge_destination_circuit_breaker_threshold
            circuit_open_until = _format_dt(_utc_now() + timedelta(seconds=self.settings.bridge_destination_circuit_open_seconds)) if circuit_open else None
            await self.community_store.mark_external_sync_event(event_id, status='dead_letter' if terminal else 'retry', error='push_failed', backoff_seconds=0 if terminal else backoff_seconds, dead_letter_reason_code='max_attempts' if terminal else '')
            await self.community_store.update_bridge_destination_state(destination=destination, circuit_state='open' if circuit_open else 'closed', consecutive_failures=failures, last_error='push_failed', circuit_open_until=circuit_open_until, success=False, metadata={'event_id': event_id, 'attempt_count': next_attempt})
    except Exception as exc:
        next_attempt = attempt_count + 1
        terminal = next_attempt >= self.settings.bridge_max_attempts
        failures = int((state or {}).get('consecutive_failures') or 0) + 1
        circuit_open = failures >= self.settings.bridge_destination_circuit_breaker_threshold
        circuit_open_until = _format_dt(_utc_now() + timedelta(seconds=self.settings.bridge_destination_circuit_open_seconds)) if circuit_open else None
        await self.community_store.mark_external_sync_event(event_id, status='dead_letter' if terminal else 'retry', error=str(exc), backoff_seconds=0 if terminal else backoff_seconds, dead_letter_reason_code='exception' if terminal else '')
        await self.community_store.update_bridge_destination_state(destination=destination, circuit_state='open' if circuit_open else 'closed', consecutive_failures=failures, last_error=str(exc), circuit_open_until=circuit_open_until, success=False, metadata={'event_id': event_id, 'attempt_count': next_attempt})
        LOGGER.warning('Failed to deliver external sync event id=%s destination=%s error=%s', event_id, destination, exc)

def _transport_content_kind(event_type: str) -> str:
    if '.announcement.' in event_type or event_type in {'announcement.created', 'announcement.updated', 'announcement.deleted', 'bridge.announcement'}:
        return 'announcement'
    if '.devlog.' in event_type or event_type in {'devlog.created', 'devlog.updated', 'devlog.deleted', 'bridge.devlog'}:
        return 'devlog'
    return ''


def _transport_content_channel_id(bot: "NMDiscordBot", content_kind: str) -> int | None:
    if content_kind == 'announcement':
        return bot.settings.discord_announcements_channel_id
    if content_kind == 'devlog':
        return bot.settings.discord_devlog_channel_id
    return None


def _build_transport_content_embed(content_kind: str, payload: dict[str, Any]) -> discord.Embed:
    title = str(payload.get('title') or f"{content_kind.title()} / NeverMine")
    description = str(payload.get('text') or payload.get('description') or payload.get('body') or '—')
    color = STAFF_COLOR if content_kind == 'announcement' else EMBED_COLOR
    embed = discord.Embed(title=title[:256], description=description[:4000], color=color)
    if payload.get('url'):
        embed.add_field(name='Ссылка', value=str(payload.get('url'))[:1000], inline=False)
    _, image_urls, attachment_lines = external_comment_attachments(payload)
    if attachment_lines:
        embed.add_field(name='Вложения', value=' | '.join(attachment_lines[:4])[:1000], inline=False)
    if image_urls:
        with contextlib.suppress(Exception):
            embed.set_image(url=image_urls[0])
    return embed


async def _fetch_message_from_channel(channel: Any, message_id: str) -> discord.Message | None:
    if channel is None or not str(message_id or '').isdigit():
        return None
    fetch_message = getattr(channel, 'fetch_message', None)
    if not callable(fetch_message):
        return None
    with contextlib.suppress(Exception):
        return await fetch_message(int(message_id))
    return None


def _safe_filename(name: str, fallback: str = 'attachment.bin') -> str:
    cleaned = re.sub(r'[^A-Za-z0-9._-]+', '-', str(name or '').strip())
    return cleaned[:120] or fallback


async def _download_external_attachment_file(url: str, filename: str, *, max_bytes: int = 3_500_000) -> discord.File | None:
    raw_url = str(url or '').strip()
    if not raw_url.startswith(('http://', 'https://')):
        return None
    def _fetch() -> tuple[bytes, str] | None:
        req = Request(raw_url, headers={'User-Agent': 'NMDiscordBot/0.31'})
        with urlopen(req, timeout=10) as response:
            data = response.read(max_bytes + 1)
            content_type = str(response.headers.get('Content-Type') or '')
        if len(data) > max_bytes:
            return None
        ext = mimetypes.guess_extension(content_type.split(';', 1)[0].strip()) if content_type else None
        effective_name = _safe_filename(filename or (Path(urlparse(raw_url).path).name if urlparse(raw_url).path else ''), fallback='attachment' + (ext or '.bin'))
        if '.' not in effective_name and ext:
            effective_name += ext
        return data, effective_name
    with contextlib.suppress(Exception):
        result = await asyncio.to_thread(_fetch)
        if result is None:
            return None
        data, effective_name = result
        return discord.File(io.BytesIO(data), filename=effective_name)
    return None


async def _send_external_attachment_followups(target: discord.abc.Messageable, payload: dict[str, Any], *, reference: discord.Message | None = None) -> None:
    normalized, image_urls, attachment_lines = external_comment_attachments(payload)
    uploaded_any = False
    for item in normalized[:3]:
        file = await _download_external_attachment_file(str(item.get('url') or ''), str(item.get('filename') or 'attachment'))
        if file is None:
            continue
        with contextlib.suppress(Exception):
            kwargs = {'file': file}
            if reference is not None:
                kwargs['reference'] = reference
                kwargs['mention_author'] = False
            await target.send(**kwargs)
            uploaded_any = True
    for image_url in image_urls[:2]:
        with contextlib.suppress(Exception):
            embed = discord.Embed(color=EMBED_COLOR)
            embed.set_image(url=image_url)
            kwargs = {'embed': embed}
            if reference is not None:
                kwargs['reference'] = reference
                kwargs['mention_author'] = False
            await target.send(**kwargs)
    if attachment_lines and not uploaded_any:
        with contextlib.suppress(Exception):
            kwargs = {'content': 'Вложения: ' + ' | '.join(attachment_lines[:5])[:1800]}
            if reference is not None:
                kwargs['reference'] = reference
                kwargs['mention_author'] = False
            await target.send(**kwargs)


async def _mirror_external_content_event(bot: "NMDiscordBot", envelope: dict[str, Any]) -> bool:
    event_type = str(envelope.get('event_type') or '')
    payload = envelope.get('payload') if isinstance(envelope.get('payload'), dict) else {}
    content_kind = _transport_content_kind(event_type)
    if not content_kind:
        return False
    channel_id = _transport_content_channel_id(bot, content_kind)
    channel = bot._get_message_channel(channel_id) if channel_id else None
    if channel is None:
        return True
    source_platform = str(payload.get('source_platform') or envelope.get('source') or 'external').strip().lower() or 'external'
    external_content_id = extract_external_content_id(payload, content_kind=content_kind)
    mirror = await bot.community_store.get_external_content_mirror(source_platform=source_platform, content_kind=content_kind, external_content_id=external_content_id) if external_content_id else None
    mirrored_message = await _fetch_message_from_channel(channel, str((mirror or {}).get('discord_message_id') or '')) if mirror else None
    if event_type.endswith('.deleted'):
        if mirrored_message is not None:
            with contextlib.suppress(Exception):
                await mirrored_message.delete()
            if external_content_id:
                await bot.community_store.delete_external_content_mirror(source_platform=source_platform, content_kind=content_kind, external_content_id=external_content_id)
            return True
        with contextlib.suppress(Exception):
            await channel.send(f'🗑️ Внешняя система удалила {content_kind}.')
        return True
    embed = _build_transport_content_embed(content_kind, payload)
    message = mirrored_message
    created = False
    if message is not None:
        with contextlib.suppress(Exception):
            await message.edit(content=None, embed=embed)
    else:
        mention = ''
        if event_type.endswith('.created'):
            guild = bot.get_guild(bot.settings.discord_guild_id) if bot.settings.discord_guild_id else (bot.guilds[0] if len(bot.guilds) == 1 else None)
            mention = await bot._subscription_event_mentions(guild, event_kind=event_type)
        with contextlib.suppress(Exception):
            message = await channel.send(content=mention or None, embed=embed)
            created = True
    if message is not None and external_content_id:
        await bot.community_store.upsert_external_content_mirror(source_platform=source_platform, content_kind=content_kind, external_content_id=external_content_id, discord_channel_id=str(getattr(channel, 'id', '') or ''), discord_message_id=str(getattr(message, 'id', '') or ''), metadata={'event_type': event_type, 'created': created, 'title': str(payload.get('title') or '')})
    return True


async def _create_external_report_message(bot: "NMDiscordBot", guild: discord.Guild, payload: dict[str, Any], *, source_platform: str, event_type: str) -> discord.Message | None:
    channel = bot._get_message_channel(bot.settings.discord_reports_channel_id)
    if channel is None:
        return None
    title = str(payload.get('title') or 'Внешний репорт')
    details = str(payload.get('details') or payload.get('body') or payload.get('text') or '—')
    embed = discord.Embed(title=title[:256], description=details[:3500], color=ERROR_COLOR)
    reporter_id = str(payload.get('reporter_id') or payload.get('actor_user_id') or '')
    target_id = str(payload.get('target_id') or payload.get('reported_user_id') or '')
    if reporter_id:
        embed.add_field(name='Внешний отправитель', value=reporter_id[:128], inline=False)
    if target_id:
        embed.add_field(name='Цель', value=target_id[:128], inline=False)
    attach_lines = external_comment_attachments(payload)[2]
    if attach_lines:
        embed.add_field(name='Вложения', value=' | '.join(attach_lines[:4])[:1000], inline=False)
    message = await channel.send(embed=embed)
    thread = None
    with contextlib.suppress(Exception):
        thread = await message.create_thread(name=f'report-{title}'[:100], auto_archive_duration=1440)
    topic_ref = str(getattr(thread, 'id', '') or getattr(message, 'id', '') or '')
    await bot.community_store.register_forum_topic(thread_id=topic_ref, guild_id=str(guild.id), forum_channel_id=str(getattr(channel, 'id', '') or ''), topic_kind='report', owner_user_id=reporter_id, title=title[:250], tags=[bot.settings.forum_tag_status_open_name], metadata={'created_via': 'external-bridge', 'source_platform': source_platform, 'event_type': event_type, 'report_root_message_id': str(getattr(message, 'id', '') or ''), **payload}, auto_close_after_seconds=bot.settings.report_escalation_hours * 3600)
    external_topic_id = extract_external_topic_id(payload) or topic_ref
    await bot.community_store.upsert_external_discussion_mirror(source_platform=source_platform, external_topic_id=external_topic_id, topic_kind='report', discord_object_id=topic_ref, channel_id=str(getattr(channel, 'id', '') or ''), metadata={'event_type': event_type, 'title': title[:128]})
    return message


async def _resolve_incoming_discussion_target(bot: "NMDiscordBot", guild: discord.Guild, *, topic_kind: str, payload: dict[str, Any], source_platform: str) -> dict[str, Any] | None:
    if topic_kind == 'report':
        channel = bot._get_message_channel(bot.settings.discord_reports_channel_id)
        if channel is None:
            return None
        object_id = str(payload.get('thread_id') or '')
        external_topic_id = extract_external_topic_id(payload)
        if external_topic_id:
            mirror = await bot.community_store.get_external_discussion_mirror(source_platform=source_platform, external_topic_id=external_topic_id)
            if mirror is not None:
                object_id = str(mirror.get('discord_object_id') or object_id)
        if object_id.isdigit():
            thread = bot.get_channel(int(object_id))
            if isinstance(thread, discord.Thread):
                record = await bot.community_store.get_forum_topic(object_id)
                return {'kind': 'thread', 'thread': thread, 'record': record, 'object_id': object_id, 'external_topic_id': external_topic_id}
        if not object_id.isdigit():
            return None
        message = await _fetch_message_from_channel(channel, object_id)
        record = await bot.community_store.get_forum_topic(object_id)
        return {'kind': 'report', 'message': message, 'channel': channel, 'record': record, 'object_id': object_id, 'external_topic_id': external_topic_id}
    thread_id = str(payload.get('thread_id') or '')
    external_topic_id = extract_external_topic_id(payload)
    if external_topic_id:
        mirror = await bot.community_store.get_external_discussion_mirror(source_platform=source_platform, external_topic_id=external_topic_id)
        if mirror is not None:
            thread_id = str(mirror.get('discord_object_id') or thread_id)
    if not thread_id.isdigit():
        return None
    thread = bot.get_channel(int(thread_id))
    if thread is None or not isinstance(thread, discord.Thread):
        return None
    record = await bot.community_store.get_forum_topic(thread_id)
    return {'kind': 'thread', 'thread': thread, 'record': record, 'object_id': thread_id, 'external_topic_id': external_topic_id}


async def handle_incoming_transport_event(self, envelope: dict[str, Any]) -> None:
    event_type = str(envelope.get('event_type') or '')
    payload = envelope.get('payload') if isinstance(envelope.get('payload'), dict) else {}
    await self.record_audit(action='incoming_transport_event', actor_user_id=None, target_user_id=None, status='success', payload={'event_type': event_type, 'event_id': envelope.get('event_id')})
    if await _mirror_external_content_event(self, envelope):
        return
    if event_type in {'community.event.created', 'event.created', 'bridge.event'}:
        channel = self._get_message_channel(self.settings.discord_events_channel_id)
        if channel is not None:
            embed = discord.Embed(title=str(payload.get('title') or f"{self.settings.nevermine_server_name} — событие"), description=str(payload.get('text') or payload.get('description') or '—'), color=EMBED_COLOR)
            await channel.send(embed=embed)
        return
    if event_type in {'identity.telegram.linked', 'identity.sync', 'identity.vk.linked', 'identity.workspace.linked'}:
        minecraft_uuid = str(payload.get('minecraft_uuid') or '')
        if minecraft_uuid:
            await self.community_store.sync_identity(
                minecraft_uuid=minecraft_uuid,
                minecraft_username=str(payload.get('minecraft_username') or payload.get('player_name') or ''),
                telegram_user_id=str(payload.get('telegram_user_id') or ''),
                telegram_username=str(payload.get('telegram_username') or ''),
                vk_user_id=str(payload.get('vk_user_id') or ''),
                workspace_actor_id=str(payload.get('workspace_actor_id') or payload.get('workspace_user_id') or ''),
                metadata=payload,
            )
        return
    if event_type in {'identity.telegram.unlinked', 'identity.vk.unlinked', 'identity.workspace.unlinked'}:
        platform_map = {
            'identity.telegram.unlinked': ('telegram', str(payload.get('telegram_user_id') or '')),
            'identity.vk.unlinked': ('vk', str(payload.get('vk_user_id') or '')),
            'identity.workspace.unlinked': ('workspace', str(payload.get('workspace_actor_id') or payload.get('workspace_user_id') or '')),
        }
        platform, platform_user_id = platform_map.get(event_type, ('', ''))
        if platform and platform_user_id:
            await self.community_store.remove_platform_link(platform=platform, platform_user_id=platform_user_id)
        return
    guild = self.get_guild(self.settings.discord_guild_id) if self.settings.discord_guild_id else (self.guilds[0] if len(self.guilds) == 1 else None)
    source_platform = str(payload.get('source_platform') or envelope.get('source') or 'external').strip().lower() or 'external'
    topic_kind_map = {
        'support': 'support',
        'bug_report': 'bug',
        'suggestion': 'suggestion',
        'appeal': 'appeal',
        'guild_recruitment': 'guild_recruitment',
        'chronicle': 'chronicle',
        'lore_discussion': 'lore_discussion',
        'report': 'report',
    }
    created_types = {
        'community.support.created',
        'community.bug_report.created',
        'community.suggestion.created',
        'community.guild_recruitment.created',
        'community.appeal.created',
        'community.chronicle.created',
        'community.lore_discussion.created',
        'community.report.created',
    }
    if event_type in created_types:
        if guild is not None:
            with contextlib.suppress(Exception):
                kind_key = event_type.split('.')[1]
                topic_kind = topic_kind_map.get(kind_key, 'support')
                external_topic_id = extract_external_topic_id(payload)
                if topic_kind == 'report':
                    message = await _create_external_report_message(self, guild, payload, source_platform=source_platform, event_type=event_type)
                    if message is not None and external_topic_id:
                        mirror = await self.community_store.get_external_discussion_mirror(source_platform=source_platform, external_topic_id=external_topic_id)
                        if mirror is None:
                            await self.community_store.upsert_external_discussion_mirror(source_platform=source_platform, external_topic_id=external_topic_id, topic_kind=topic_kind, discord_object_id=str(message.id), channel_id=str(getattr(message.channel, 'id', '') or ''), metadata={'event_type': event_type})
                else:
                    thread = await _create_external_forum_topic(self, guild, topic_kind=topic_kind, title=str(payload.get('title') or event_type), body=str(payload.get('body') or payload.get('text') or payload.get('description') or payload.get('details') or '—'), actor_user_id=str(payload.get('actor_user_id') or payload.get('reporter_id') or '0'), actor_name=str(payload.get('actor_name') or payload.get('source') or 'external'), metadata=payload)
                    if thread is not None and external_topic_id:
                        await self.community_store.upsert_external_discussion_mirror(source_platform=source_platform, external_topic_id=external_topic_id, topic_kind=topic_kind, discord_object_id=str(thread.id), channel_id=str(getattr(thread.parent, 'id', '') or ''), metadata={'event_type': event_type})
        await self.record_audit(action='incoming_bridge_discussion', actor_user_id=None, target_user_id=None, status='success', payload={'event_type': event_type, 'title': payload.get('title') or ''})
        return
    if event_type == 'community.world_signal.created':
        if guild is not None:
            channel = _resolve_layout_channel(self, guild, 'world_signals')
            if channel is not None and isinstance(channel, discord.abc.Messageable):
                embed = discord.Embed(title=str(payload.get('title') or 'World signal'), description=str(payload.get('body') or payload.get('text') or '—'), color=EMBED_COLOR)
                mentions = await self._subscription_event_mentions(guild, event_kind='community.world_signal.created')
                await channel.send(content=mentions or None, embed=embed)
        return
    if not event_type.startswith('community.') or guild is None:
        return
    parts = event_type.split('.')
    if len(parts) < 3:
        return
    kind_key = parts[1]
    action = '.'.join(parts[2:])
    topic_kind = topic_kind_map.get(kind_key)
    if topic_kind is None:
        return
    target = await _resolve_incoming_discussion_target(self, guild, topic_kind=topic_kind, payload=payload, source_platform=source_platform)
    if target is None:
        return
    object_id = str(target.get('object_id') or '')
    record = target.get('record') if isinstance(target.get('record'), dict) else (await self.community_store.get_forum_topic(object_id) if object_id else None)
    metadata = dict((record or {}).get('metadata_json') or {})
    if action == 'updated':
        new_title = str(payload.get('title') or '').strip()
        new_body = str(payload.get('body') or payload.get('text') or payload.get('description') or '').strip()
        if target['kind'] == 'thread':
            if new_title:
                with contextlib.suppress(Exception):
                    await target['thread'].edit(name=new_title[:100])
            if new_body:
                starter = None
                with contextlib.suppress(Exception):
                    starter = await target['thread'].fetch_message(target['thread'].id)
                if starter is not None:
                    with contextlib.suppress(Exception):
                        await starter.edit(content=new_body[:1900])
                else:
                    with contextlib.suppress(Exception):
                        await target['thread'].send(f'Обновление внешней темы: {new_body[:1800]}')
        else:
            message = target.get('message')
            if message is not None and getattr(message, 'embeds', None):
                embed = message.embeds[0].copy()
                if new_title:
                    embed.title = new_title[:256]
                if new_body:
                    embed.description = new_body[:4000]
                with contextlib.suppress(Exception):
                    await message.edit(embed=embed)
        metadata.update({k: v for k, v in payload.items() if k not in {'body', 'text'}})
        await self.community_store.update_forum_topic_state(thread_id=object_id, status=str((record or {}).get('status') or 'open'), tags=list((record or {}).get('tags_json') or []), metadata=metadata, closed=bool((record or {}).get('closed_at')))
        return
    if action in {'closed', 'reopened', 'paused', 'bumped', 'status_changed'}:
        status_map = {
            'closed': 'closed',
            'reopened': 'open',
            'paused': 'in_review',
            'bumped': 'open',
            'status_changed': str(payload.get('status') or 'open').strip().lower() or 'open',
        }
        status = status_map[action]
        metadata.update({k: v for k, v in payload.items() if k not in {'body', 'text'}})
        if target['kind'] == 'thread':
            await self.apply_thread_status(thread=target['thread'], topic_kind=topic_kind, status=status, metadata=metadata)
        else:
            await self.community_store.update_forum_topic_state(thread_id=object_id, status=status, tags=list((record or {}).get('tags_json') or []), metadata=metadata, closed=status == 'closed')
            if target.get('message') is not None:
                with contextlib.suppress(Exception):
                    await target['channel'].send(f'Статус внешнего репорта `{object_id}` изменён: {status}.', reference=target['message'], mention_author=False)
        return
    if action in {'claimed', 'owner_changed'}:
        staff_owner_user_id = str(payload.get('staff_owner_user_id') or payload.get('owner_user_id') or payload.get('actor_user_id') or '')
        if staff_owner_user_id:
            await self.community_store.assign_forum_topic_owner(thread_id=object_id, staff_user_id=staff_owner_user_id, staff_name=str(payload.get('actor_name') or payload.get('staff_owner_name') or 'external'))
            if target['kind'] == 'thread':
                await self.notify_topic_owner_change(thread=target['thread'], new_owner_user_id=staff_owner_user_id, actor_name=str(payload.get('actor_name') or 'external'))
            elif target.get('message') is not None:
                with contextlib.suppress(Exception):
                    await target['channel'].send(f'Ответственный по внешнему репорту изменён: <@{staff_owner_user_id}>.', reference=target['message'], mention_author=False)
        return
    if action == 'unclaimed':
        metadata['staff_owner_user_id'] = ''
        await self.community_store.update_forum_topic_state(thread_id=object_id, status=str((record or {}).get('status') or 'open'), tags=list((record or {}).get('tags_json') or []), metadata=metadata, closed=False)
        with contextlib.suppress(Exception):
            target_channel = target.get('thread') if target['kind'] == 'thread' else target.get('channel')
            if target_channel is not None:
                kwargs = {'content': 'Ответственный за тему был снят внешней системой.'}
                if target.get('message') is not None and target['kind'] != 'thread':
                    kwargs['reference'] = target['message']
                    kwargs['mention_author'] = False
                await target_channel.send(**kwargs)
        return
    if action in {'comment.appended', 'comment_added', 'comment.edited', 'comment.deleted'}:
        external_comment_id = extract_external_comment_id(payload)
        rendered_comment = render_external_comment_body(payload)
        mirror = await self.community_store.get_bridge_comment_mirror_by_external(thread_id=object_id, external_comment_id=external_comment_id, source_platform=source_platform) if external_comment_id else None
        mirrored_message = None
        fetch_message = getattr(target.get('thread') if target['kind'] == 'thread' else target.get('channel'), 'fetch_message', None)
        if mirror is not None and callable(fetch_message):
            with contextlib.suppress(Exception):
                mirrored_message = await fetch_message(int(mirror.get('discord_message_id') or 0))
        send_target = target.get('thread') if target['kind'] == 'thread' else target.get('channel')
        reference_message = target.get('message') if target['kind'] == 'report' else None
        if action in {'comment.appended', 'comment_added'}:
            if mirrored_message is not None:
                with contextlib.suppress(Exception):
                    await mirrored_message.edit(content=rendered_comment[:1900])
                return
            with contextlib.suppress(Exception):
                kwargs = {'content': rendered_comment[:1900]}
                if reference_message is not None:
                    kwargs['reference'] = reference_message
                    kwargs['mention_author'] = False
                sent_message = await send_target.send(**kwargs)
                await _send_external_attachment_followups(send_target, payload, reference=sent_message if isinstance(sent_message, discord.Message) else reference_message)
                if external_comment_id:
                    await self.community_store.upsert_bridge_comment_mirror(thread_id=object_id, source_platform=source_platform, external_comment_id=external_comment_id, discord_message_id=str(getattr(sent_message, 'id', '')), metadata={'event_type': event_type, 'actor_name': str(payload.get('actor_name') or payload.get('source') or 'external')})
            return
        if action == 'comment.edited':
            if mirrored_message is not None:
                with contextlib.suppress(Exception):
                    await mirrored_message.edit(content=rendered_comment[:1900])
                    await self.community_store.upsert_bridge_comment_mirror(thread_id=object_id, source_platform=source_platform, external_comment_id=external_comment_id, discord_message_id=str(getattr(mirrored_message, 'id', '')), metadata={'event_type': event_type, 'edited_at': str(payload.get('edited_at') or '')})
                    return
            note = build_external_comment_notice(action, payload)
            with contextlib.suppress(Exception):
                kwargs = {'content': (note + '\n' + rendered_comment[:1800])[:1950]}
                if reference_message is not None:
                    kwargs['reference'] = reference_message
                    kwargs['mention_author'] = False
                sent_message = await send_target.send(**kwargs)
                await _send_external_attachment_followups(send_target, payload, reference=sent_message if isinstance(sent_message, discord.Message) else reference_message)
                if external_comment_id:
                    await self.community_store.upsert_bridge_comment_mirror(thread_id=object_id, source_platform=source_platform, external_comment_id=external_comment_id, discord_message_id=str(getattr(sent_message, 'id', '')), metadata={'event_type': event_type, 'edited_at': str(payload.get('edited_at') or '')})
            return
        if action == 'comment.deleted':
            if mirrored_message is not None:
                with contextlib.suppress(Exception):
                    await mirrored_message.delete()
                if external_comment_id:
                    await self.community_store.delete_bridge_comment_mirror(thread_id=object_id, external_comment_id=external_comment_id, source_platform=source_platform)
                return
            note = build_external_comment_notice(action, payload)
            with contextlib.suppress(Exception):
                kwargs = {'content': note[:1950]}
                if reference_message is not None:
                    kwargs['reference'] = reference_message
                    kwargs['mention_author'] = False
                await send_target.send(**kwargs)
            return

async def handle_incoming_admin_event(self, envelope: dict[str, Any]) -> dict[str, Any]:
    payload = envelope.get('payload') if isinstance(envelope.get('payload'), dict) else {}
    event_type = str(envelope.get('event_type') or '')
    if event_type == 'admin.approval.create':
        request_id = await self.community_store.create_approval_request(
            kind=str(payload.get('kind') or 'external_admin'),
            payload=payload,
            requested_by=str(envelope.get('actor_user_id') or '0'),
            requested_by_name=str(payload.get('requested_by_name') or 'external-admin'),
            required_role=str(payload.get('required_role') or 'admin'),
        )
        return build_signed_response(action='approval.create', ok=True, payload={'request_id': request_id})
    await self.record_audit(action='incoming_admin_event', actor_user_id=None, target_user_id=None, status='success', payload={'event_type': event_type, 'event_id': envelope.get('event_id')})
    return build_signed_response(action=event_type or 'unknown', ok=True, payload={'accepted': True})

def _get_forum_channel(self, channel_id: int | None) -> discord.ForumChannel | None:
    if not channel_id:
        return None
    channel = self.get_channel(channel_id)
    return channel if isinstance(channel, discord.ForumChannel) else None

def _get_stage_channel(self, channel_id: int | None) -> discord.StageChannel | None:
    if not channel_id:
        return None
    channel = self.get_channel(channel_id)
    return channel if isinstance(channel, discord.StageChannel) else None

def _interest_role_ids(self) -> dict[str, int]:
    return {
        name: role_id
        for name, role_id in {
            'news': self.settings.interest_role_news_id,
            'lore': self.settings.interest_role_lore_id,
            'gameplay': self.settings.interest_role_gameplay_id,
            'events': self.settings.interest_role_events_id,
            'guilds': self.settings.interest_role_guilds_id,
            'media': self.settings.interest_role_media_id,
            'devlogs': self.settings.interest_role_devlogs_id,
        }.items()
        if role_id
    }

async def publish_panel(self, *, guild_id: int, panel_type: str, actor_user_id: int | None = None) -> discord.Message:
    panel_type = panel_type.strip().lower()
    if panel_type == 'onboarding':
        channel_id = self.settings.discord_start_here_channel_id
        embed = build_onboarding_embed(self.settings)
        view = OnboardingView(self)
    elif panel_type == 'interest_roles':
        channel_id = self.settings.discord_roles_channel_id
        embed = build_interest_roles_embed(self.settings)
        view = InterestRolesView(self)
    elif panel_type == 'help':
        channel_id = self.settings.discord_faq_channel_id
        embed = build_help_embed(self.settings)
        view = HelpPanelView(self)
    else:
        raise ValueError(f'unsupported panel_type={panel_type}')
    channel = self._get_message_channel(channel_id)
    if channel is None:
        raise RuntimeError(f'panel channel is not configured or unavailable for {panel_type}')
    binding = await self.community_store.get_panel_binding(guild_id=str(guild_id), panel_type=panel_type)
    message = None
    if binding is not None:
        bound_channel = self.get_channel(int(binding.get('channel_id') or channel_id or 0)) or channel
        fetch_message = getattr(bound_channel, 'fetch_message', None)
        if callable(fetch_message):
            try:
                message = await fetch_message(int(binding.get('message_id') or 0))
            except Exception:
                message = None
    if message is None:
        message = await channel.send(embed=embed, view=view)
    else:
        await message.edit(embed=embed, view=view)
    await self.community_store.upsert_panel_binding(
        guild_id=str(guild_id),
        panel_type=panel_type,
        channel_id=str(getattr(message.channel, 'id', channel_id or '')),
        message_id=str(message.id),
        version=new_version,
        metadata={'panel_type': panel_type, 'content': get_panel_content(self.settings, panel_type)},
    )
    await self.record_audit(action='panel_publish', actor_user_id=actor_user_id, target_user_id=None, status='success', payload={'guild_id': guild_id, 'panel_type': panel_type, 'channel_id': getattr(message.channel, 'id', None), 'message_id': message.id})
    return message


async def _resolve_thread(self, thread_id: str) -> discord.Thread | None:
    if not thread_id.isdigit():
        return None
    channel = self.get_channel(int(thread_id))
    if isinstance(channel, discord.Thread):
        return channel
    try:
        fetched = await self.fetch_channel(int(thread_id))
    except Exception:
        return None
    return fetched if isinstance(fetched, discord.Thread) else None

async def _reconcile_panels(self, guild_id: int) -> None:
    for panel_type in ('onboarding', 'interest_roles', 'help'):
        try:
            await self.publish_panel(guild_id=guild_id, panel_type=panel_type, actor_user_id=None)
        except Exception:
            LOGGER.exception('Failed to reconcile panel %s', panel_type)

async def _send_stage_announcement(self, *, title: str, description: str, starts_at: str, actor_user_id: int | None) -> int:
    embed = discord.Embed(title=title, description=description, color=STAFF_COLOR)
    embed.add_field(name="Stage", value=f"<#{self.settings.discord_stage_channel_id}>" if self.settings.discord_stage_channel_id else "не настроено", inline=False)
    if starts_at:
        embed.add_field(name="Когда", value=starts_at, inline=True)
    guild = self.get_guild(self.settings.discord_guild_id) if self.settings.discord_guild_id else (self.guilds[0] if len(self.guilds) == 1 else None)
    role_mentions = self._interest_ping_mentions('stage_announce')
    subscription_mentions = await self._subscription_event_mentions(guild, event_kind='community.stage.announcement')
    mention = ' '.join(part for part in [role_mentions, subscription_mentions] if part).strip()
    sent = 0
    for channel_id in [self.settings.discord_announcements_channel_id, self.settings.discord_events_channel_id]:
        channel = self._get_message_channel(channel_id)
        if channel is not None:
            await channel.send(content=mention or None, embed=embed)
            sent += 1
    await self.record_audit(action="stage_announce", actor_user_id=actor_user_id, target_user_id=None, status="success", payload={"title": title, "starts_at": starts_at or "", "sent": sent, "subscription_mentions": subscription_mentions.count('<@')})
    await self.queue_bridge_event("community.stage.announcement", {"title": title, "description": description, "starts_at": starts_at or "", "actor_user_id": str(actor_user_id or 0)})
    return sent

async def _send_event_reminder(self, *, title: str, description: str, starts_at: str, actor_user_id: int | None) -> None:
    channel = self._get_message_channel(self.settings.discord_events_channel_id)
    if channel is None:
        return
    embed = discord.Embed(title=title, description=description, color=EMBED_COLOR)
    if starts_at:
        embed.add_field(name="Когда", value=starts_at, inline=False)
    guild = self.get_guild(self.settings.discord_guild_id) if self.settings.discord_guild_id else (self.guilds[0] if len(self.guilds) == 1 else None)
    role_mentions = self._interest_ping_mentions('event_reminder')
    subscription_mentions = await self._subscription_event_mentions(guild, event_kind='community.event.reminder')
    mention = ' '.join(part for part in [role_mentions, subscription_mentions] if part).strip()
    await channel.send(content=(mention or None), embed=embed)
    await self.record_audit(action="event_reminder", actor_user_id=actor_user_id, target_user_id=None, status="success", payload={"title": title, "starts_at": starts_at or "", "subscription_mentions": subscription_mentions.count('<@')})
    await self.queue_bridge_event("community.event.reminder", {"title": title, "description": description, "starts_at": starts_at or "", "actor_user_id": str(actor_user_id or 0)})

async def send_staff_digest(self, *, channel: discord.abc.Messageable, actor_user_id: int | None = None) -> bool:
    overdue = await self.community_store.list_topics_needing_escalation(topic_kind='support', older_than_hours=self.settings.support_escalation_hours, limit=10)
    appeals = await self.community_store.list_topics_needing_escalation(topic_kind='appeal', older_than_hours=self.settings.appeal_escalation_hours, limit=10)
    failed_bridge = await self.community_store.list_failed_external_sync_events(limit=10)
    stale_approvals = await self.community_store.list_expired_pending_approval_requests(limit=50)
    digest_lines: list[str] = []
    if overdue:
        digest_lines.append(f'Поддержка требует внимания: {len(overdue)}')
    if appeals:
        digest_lines.append(f'Апелляции требуют внимания: {len(appeals)}')
    if failed_bridge:
        digest_lines.append(f'Проблемные bridge-события: {len(failed_bridge)}')
    if stale_approvals:
        digest_lines.append(f'Просроченные согласования: {len(stale_approvals)}')
    if not digest_lines:
        return False
    embed = discord.Embed(title=get_ops_text(self.settings, 'staff_digest_title', 'Сводка staff по NeverMine Discord'), color=STAFF_COLOR, description='\n'.join(f'• {line}' for line in digest_lines))
    await channel.send(embed=embed)
    await self.record_audit(action='staff_digest_sent', actor_user_id=actor_user_id, target_user_id=None, status='success', payload={'summary': digest_lines})
    return True

async def _scheduler_loop(self) -> None:
    while True:
        try:
            lock_name = 'scheduler-loop'
            lock_token = await self.storage.acquire_lock(lock_name)
            runtime_lock = await self.community_store.acquire_runtime_lock(lock_name, self.runtime_owner, ttl_seconds=max(30, self.settings.scheduler_poll_interval_seconds * 2))
            if self.storage.cache.client is not None and not lock_token:
                await asyncio.sleep(self.settings.scheduler_poll_interval_seconds)
                continue
            if not runtime_lock:
                await self.storage.release_lock(lock_name, lock_token)
                await asyncio.sleep(self.settings.scheduler_poll_interval_seconds)
                continue
            try:
                rows = await self.community_store.list_due_scheduled_jobs(limit=25)
                for row in rows:
                    payload = row.get('payload_json') or {}
                    job_type = str(row.get('job_type') or '')
                    attempt_count = int(row.get('attempt_count') or 0)
                    try:
                        if job_type == 'stage_announce':
                            await self._send_stage_announcement(title=str(payload.get('title') or 'NeverMine stage'), description=str(payload.get('description') or ''), starts_at=str(payload.get('starts_at') or ''), actor_user_id=int(row.get('created_by') or 0) if str(row.get('created_by') or '').isdigit() else None)
                        elif job_type == 'event_reminder':
                            await self._send_event_reminder(title=str(payload.get('title') or 'NeverMine event'), description=str(payload.get('description') or ''), starts_at=str(payload.get('starts_at') or ''), actor_user_id=int(row.get('created_by') or 0) if str(row.get('created_by') or '').isdigit() else None)
                        elif job_type == 'staff_digest':
                            destination = self._get_message_channel(int(row.get('channel_id') or 0)) if str(row.get('channel_id') or '').isdigit() else None
                            if destination is not None:
                                await self.send_staff_digest(channel=destination, actor_user_id=int(row.get('created_by') or 0) if str(row.get('created_by') or '').isdigit() else None)
                        elif job_type == 'targeted_digest':
                            destination = self._get_message_channel(int(row.get('channel_id') or 0)) if str(row.get('channel_id') or '').isdigit() else None
                            guild = self.get_guild(int(row.get('guild_id') or 0)) if str(row.get('guild_id') or '').isdigit() else (self.guilds[0] if len(self.guilds) == 1 else None)
                            if destination is not None:
                                await self._send_targeted_digest(digest_kind=str(payload.get('digest_kind') or 'staff'), channel=destination, guild=guild, actor_user_id=int(row.get('created_by') or 0) if str(row.get('created_by') or '').isdigit() else None)
                        await self.community_store.mark_scheduled_job(int(row.get('id') or 0), status='sent')
                        recurring = next_recurring_schedule(job_type=job_type, payload=payload, current_run_at=str(row.get('run_at') or ''), guild_id=str(row.get('guild_id') or ''), channel_id=str(row.get('channel_id') or ''))
                        if recurring is not None:
                            next_run_at, next_payload, next_dedupe_key = recurring
                            await self.community_store.schedule_job(job_type=job_type, run_at=next_run_at, payload=next_payload, guild_id=str(row.get('guild_id') or ''), channel_id=str(row.get('channel_id') or ''), created_by=str(row.get('created_by') or ''), dedupe_key=next_dedupe_key)
                            await self.record_audit(action='scheduled_job_recurred', actor_user_id=int(row.get('created_by') or 0) if str(row.get('created_by') or '').isdigit() else None, target_user_id=None, status='success', payload={'job_id': int(row.get('id') or 0), 'job_type': job_type, 'next_run_at': next_run_at, 'recurrence_hours': int(next_payload.get('recurrence_hours') or 0), 'remaining_occurrences': next_payload.get('remaining_occurrences')})
                        await self.set_runtime_marker('last_scheduler_success', {'job_id': str(row.get('id') or ''), 'job_type': job_type, 'at': _format_dt(_utc_now())})
                    except Exception as exc:
                        LOGGER.exception('Scheduled job failed: %s', row.get('id'))
                        next_attempt = attempt_count + 1
                        backoff_seconds = _retry_backoff(next_attempt, base_seconds=self.settings.scheduler_retry_backoff_base_seconds, max_seconds=self.settings.scheduler_retry_backoff_max_seconds)
                        terminal = next_attempt >= self.settings.scheduler_max_attempts
                        await self.community_store.mark_scheduled_job(int(row.get('id') or 0), status='dead_letter' if terminal else 'retry', error=str(exc), backoff_seconds=0 if terminal else backoff_seconds, dead_letter_reason_code='max_attempts' if terminal else '')
                await asyncio.sleep(self.settings.scheduler_poll_interval_seconds)
            finally:
                await self.community_store.release_runtime_lock(lock_name, self.runtime_owner)
                await self.storage.release_lock(lock_name, lock_token)
        except asyncio.CancelledError:
            LOGGER.info('Scheduler loop cancelled')
            raise
        except Exception:
            LOGGER.exception('Scheduler loop failed unexpectedly')
            await asyncio.sleep(min(30, self.settings.scheduler_poll_interval_seconds))

async def _send_staff_notice(self, text: str) -> bool:
    channel_ids = [
        self.settings.discord_bot_logs_channel_id,
        self.settings.discord_ops_audit_channel_id,
        self.settings.discord_audit_channel_id,
    ]
    for channel_id in channel_ids:
        channel = self._get_message_channel(channel_id) if channel_id else None
        if channel is None:
            continue
        with contextlib.suppress(Exception):
            await channel.send(text[:1900])
            return True
    return False


async def _approval_expiry_loop(self) -> None:
    while True:
        try:
            lock_name = 'approval-expiry-loop'
            lock_token = await self.storage.acquire_lock(lock_name)
            runtime_lock = await self.community_store.acquire_runtime_lock(lock_name, self.runtime_owner, ttl_seconds=max(30, self.settings.scheduler_poll_interval_seconds * 2))
            if self.storage.cache.client is not None and not lock_token:
                await asyncio.sleep(self.settings.scheduler_poll_interval_seconds)
                continue
            if not runtime_lock:
                await self.storage.release_lock(lock_name, lock_token)
                await asyncio.sleep(self.settings.scheduler_poll_interval_seconds)
                continue
            try:
                expired_rows = await self.community_store.list_expired_pending_approval_requests(limit=25)
                expired = await self.community_store.expire_pending_approval_requests(limit=25, acted_by='system-expiry-sweeper')
                for request_id in expired:
                    await self.record_audit(action='approval_request_expired', actor_user_id=None, target_user_id=None, status='warning', payload={'request_id': request_id, 'source': 'approval_expiry_loop'})
                if expired:
                    kind_counts = Counter(str(row.get('kind') or 'unknown') for row in expired_rows)
                    summary = ', '.join(f"{kind}:{count}" for kind, count in kind_counts.items()) or 'unknown'
                    await self._send_staff_notice(f"⏱️ Просрочено approval-запросов: {len(expired)}. Kinds: {summary}. ID: {', '.join(str(item) for item in expired[:10])}")
                await asyncio.sleep(self.settings.scheduler_poll_interval_seconds)
            finally:
                await self.community_store.release_runtime_lock(lock_name, self.runtime_owner)
                await self.storage.release_lock(lock_name, lock_token)
        except asyncio.CancelledError:
            LOGGER.info('Approval expiry loop cancelled')
            raise
        except Exception:
            LOGGER.exception('Approval expiry loop failed unexpectedly')
            await asyncio.sleep(min(30, self.settings.scheduler_poll_interval_seconds))


async def _run_rules_reacceptance_cycle(self) -> None:
    if not self.settings.rules_reacceptance_enforcement_enabled and self.settings.rules_reacceptance_reminder_hours < 1:
        return
    guild = self.get_guild(self.settings.discord_guild_id) if self.settings.discord_guild_id else (self.guilds[0] if len(self.guilds) == 1 else None)
    if guild is None:
        return
    guild_id = str(guild.id)
    rows = await self.community_store.list_rules_reacceptance_candidates(guild_id=guild_id, current_rules_version=self.settings.rules_version, limit=200)
    if not rows:
        return
    now = datetime.now(timezone.utc)
    reminder_count = 0
    enforced_count = 0
    visitor_role = guild.get_role(self.settings.visitor_role_id) if self.settings.visitor_role_id else None
    member_role = guild.get_role(self.settings.member_role_id) if self.settings.member_role_id else None
    bot_member = guild.me
    for row in rows:
        user_id = str(row.get('discord_user_id') or '').strip()
        if not user_id.isdigit():
            continue
        state = rules_reacceptance_state(
            row,
            current_rules_version=self.settings.rules_version,
            grace_hours=self.settings.rules_reacceptance_grace_hours,
            reminder_hours=self.settings.rules_reacceptance_reminder_hours,
            now=now,
        )
        metadata = dict(state.get('metadata') or {})
        member = guild.get_member(int(user_id))
        changed_metadata = False
        if state.get('reminder_due'):
            if member is not None:
                with contextlib.suppress(Exception):
                    await member.send(f'На сервере NeverMine обновилась версия правил: `{self.settings.rules_version}`. Повтори принятие правил через onboarding-панель в Discord.')
            metadata['rules_reacceptance_nudged_at'] = _format_dt(_utc_now())
            metadata['rules_reacceptance_target_version'] = self.settings.rules_version
            changed_metadata = True
            reminder_count += 1
            await self.record_audit(action='rules_reacceptance_nudged', actor_user_id=None, target_user_id=int(user_id), status='warning', payload={'rules_version': self.settings.rules_version, 'source': 'rules_reacceptance_loop'})
        if state.get('enforcement_due') and member is not None and bot_member is not None:
            bot_can_manage = bot_member.guild_permissions.manage_roles
            if bot_can_manage:
                if visitor_role is not None and bot_member.top_role.position > visitor_role.position and visitor_role not in member.roles:
                    with contextlib.suppress(Exception):
                        await member.add_roles(visitor_role, reason='NeverMine rules reacceptance enforcement')
                if member_role is not None and bot_member.top_role.position > member_role.position and member_role in member.roles:
                    with contextlib.suppress(Exception):
                        await member.remove_roles(member_role, reason='NeverMine rules reacceptance enforcement')
                metadata['rules_reacceptance_enforced_at'] = _format_dt(_utc_now())
                metadata['rules_reacceptance_target_version'] = self.settings.rules_version
                metadata['rules_reacceptance_state'] = 'restricted'
                changed_metadata = True
                enforced_count += 1
                await self.record_audit(action='rules_reacceptance_enforced', actor_user_id=None, target_user_id=int(user_id), status='warning', payload={'rules_version': self.settings.rules_version, 'source': 'rules_reacceptance_loop'})
        if changed_metadata:
            await self.community_store.update_rules_acceptance_metadata(guild_id=guild_id, discord_user_id=user_id, metadata=metadata)
    if reminder_count or enforced_count:
        summary = f'📋 Rules reacceptance: reminders={reminder_count}, enforced={enforced_count}, rules_version={self.settings.rules_version}'
        await self._send_staff_notice(summary)


async def _rules_reacceptance_loop(self) -> None:
    interval = max(60, int(self.settings.rules_reacceptance_check_interval_seconds or self.settings.scheduler_poll_interval_seconds))
    while True:
        try:
            lock_name = 'rules-reacceptance-loop'
            lock_token = await self.storage.acquire_lock(lock_name)
            runtime_lock = await self.community_store.acquire_runtime_lock(lock_name, self.runtime_owner, ttl_seconds=max(30, interval * 2))
            if self.storage.cache.client is not None and not lock_token:
                await asyncio.sleep(interval)
                continue
            if not runtime_lock:
                await self.storage.release_lock(lock_name, lock_token)
                await asyncio.sleep(interval)
                continue
            try:
                await self._run_rules_reacceptance_cycle()
                await asyncio.sleep(interval)
            finally:
                await self.community_store.release_runtime_lock(lock_name, self.runtime_owner)
                await self.storage.release_lock(lock_name, lock_token)
        except asyncio.CancelledError:
            LOGGER.info('Rules reacceptance loop cancelled')
            raise
        except Exception:
            LOGGER.exception('Rules reacceptance loop failed unexpectedly')
            await asyncio.sleep(min(30, interval))

async def _run_escalation_cycle(self) -> None:
    checks = [
        ('support', self.settings.support_escalation_hours),
        ('appeal', self.settings.appeal_escalation_hours),
        ('report', self.settings.report_escalation_hours),
    ]
    for topic_kind, hours in checks:
        for topic in await self.community_store.list_topics_needing_escalation(topic_kind=topic_kind, older_than_hours=hours, limit=10):
            await self._send_topic_escalation(topic_kind=topic_kind, topic=topic)
    for topic in await self.community_store.list_topics_needing_escalation(topic_kind='guild_recruitment', older_than_hours=max(1, self.settings.forum_recruitment_auto_close_hours - self.settings.guild_recruitment_expiry_warning_hours), limit=10):
        metadata = topic.get('metadata_json') or {}
        if metadata.get('expiry_warning_sent_at'):
            continue
        thread = await self._resolve_thread(str(topic.get('thread_id') or ''))
        if thread is not None and isinstance(thread, discord.Thread):
            try:
                await thread.send('Внимание: тема набора скоро будет автоматически архивирована из-за неактивности. При необходимости обновите её командой `/guild_recruit_bump`.')
            except Exception:
                LOGGER.exception('Failed to send recruitment expiry warning for thread %s', topic.get('thread_id'))
        metadata['expiry_warning_sent_at'] = _format_dt(_utc_now())
        await self.community_store.update_forum_topic_state(thread_id=str(topic.get('thread_id')), status=str(topic.get('status') or 'open'), tags=list(topic.get('tags_json') or []), metadata=metadata, closed=False)

async def _send_topic_escalation(self, *, topic_kind: str, topic: dict[str, Any]) -> None:
    thread_id = str(topic.get('thread_id') or '')
    guild_id = str(topic.get('guild_id') or '')
    if not thread_id:
        return
    channel = self._get_message_channel(self.settings.discord_reports_channel_id) or self._get_message_channel(self.settings.discord_bot_logs_channel_id)
    if channel is not None:
        thread_ref = f"<#{thread_id}>" if thread_id.isdigit() else thread_id
        metadata = topic.get('metadata_json') or {}
        owner = metadata.get('staff_owner_user_id')
        owner_ref = f"<@{owner}>" if owner else 'не назначен'
        await channel.send(f"Эскалация: тема `{topic_kind}` ждёт реакции команды слишком долго. Тема: {thread_ref}. Ответственный: {owner_ref}.")
    await self.community_store.mark_topic_escalated(thread_id=thread_id, reason=f'{topic_kind}_sla')
    await self.record_audit(action='forum_topic_escalated', actor_user_id=None, target_user_id=None, status='warning', payload={'thread_id': thread_id, 'topic_kind': topic_kind, 'guild_id': guild_id})

def _get_audit_channel_ids(self, category: str) -> list[int]:
    ids: list[int] = []
    if category == "security" and self.settings.discord_security_audit_channel_id:
        ids.append(self.settings.discord_security_audit_channel_id)
    elif category == "business" and self.settings.discord_business_audit_channel_id:
        ids.append(self.settings.discord_business_audit_channel_id)
    elif category == "ops" and self.settings.discord_ops_audit_channel_id:
        ids.append(self.settings.discord_ops_audit_channel_id)
    if self.settings.discord_audit_channel_id:
        ids.append(self.settings.discord_audit_channel_id)
    return ids

def _get_message_channel(self, channel_id: int | None) -> discord.abc.Messageable | None:
    if not channel_id:
        return None
    channel = self.get_channel(channel_id)
    if channel is None:
        return None
    return channel if isinstance(channel, discord.abc.Messageable) else None


def _build_state_restore_plan(payload: dict[str, Any], section: str) -> dict[str, Any]:
    plan = {'sections': [], 'counts': {}, 'idempotent_keys': {}}
    if section in {'all', 'maintenance'} and isinstance(payload.get('maintenance_mode'), dict):
        plan['sections'].append('maintenance_mode')
    if section in {'all', 'forum_policies'} and isinstance(payload.get('runtime_forum_policy_overrides'), dict):
        plan['sections'].append('runtime_forum_policy_overrides')
        plan['counts']['forum_policy_kinds'] = len(payload.get('runtime_forum_policy_overrides') or {})
    if section in {'all', 'panel_registry'} and isinstance(payload.get('panel_registry'), list):
        plan['sections'].append('panel_registry')
        plan['counts']['panel_registry'] = len(payload.get('panel_registry') or [])
    if section in {'all', 'layout_alias_bindings'} and isinstance(payload.get('layout_alias_bindings'), list):
        plan['sections'].append('layout_alias_bindings')
        plan['counts']['layout_alias_bindings'] = len(payload.get('layout_alias_bindings') or [])
    if section in {'all', 'topics'} and isinstance(payload.get('topics'), list):
        plan['sections'].append('topics')
        plan['counts']['topics'] = len(payload.get('topics') or [])
        plan['idempotent_keys']['topics'] = [str(row.get('thread_id') or '') for row in (payload.get('topics') or [])[:50] if str(row.get('thread_id') or '')]
    if section in {'all', 'scheduled_jobs'} and isinstance(payload.get('scheduled_jobs'), list):
        plan['sections'].append('scheduled_jobs')
        jobs = payload.get('scheduled_jobs') or []
        plan['counts']['scheduled_jobs'] = len(jobs)
        dedupe_keys = []
        for row in jobs[:50]:
            key = str(row.get('dedupe_key') or '')
            if not key:
                base = {
                    'job_type': str(row.get('job_type') or ''),
                    'guild_id': str(row.get('guild_id') or ''),
                    'channel_id': str(row.get('channel_id') or ''),
                    'run_at': str(row.get('run_at') or ''),
                    'payload_json': row.get('payload_json') or {},
                }
                key = hashlib.sha256(json.dumps(base, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
            dedupe_keys.append(key)
        plan['idempotent_keys']['scheduled_jobs'] = dedupe_keys[:50]
    if section in {'all', 'failed_bridge_events'} and isinstance(payload.get('failed_bridge_events'), list):
        plan['sections'].append('failed_bridge_events')
        events = payload.get('failed_bridge_events') or []
        plan['counts']['failed_bridge_events'] = len(events)
        dedupe_keys = []
        for row in events[:50]:
            key = str(row.get('dedupe_key') or '')
            if not key:
                base = {
                    'event_kind': str(row.get('event_kind') or ''),
                    'destination': str(row.get('destination') or ''),
                    'payload_json': row.get('payload_json') or {},
                }
                key = hashlib.sha256(json.dumps(base, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
            dedupe_keys.append(key)
        plan['idempotent_keys']['failed_bridge_events'] = dedupe_keys[:50]
    if section in {'all', 'bridge_destination_state'} and isinstance(payload.get('bridge_destination_state'), list):
        plan['sections'].append('bridge_destination_state')
        plan['counts']['bridge_destination_state'] = len(payload.get('bridge_destination_state') or [])
        plan['idempotent_keys']['bridge_destination_state'] = [str(row.get('destination') or '') for row in (payload.get('bridge_destination_state') or [])[:50] if str(row.get('destination') or '')]
    if section in {'all', 'schema_meta'} and isinstance(payload.get('schema_meta'), dict):
        plan['sections'].append('schema_meta')
        plan['counts']['schema_meta'] = len(payload.get('schema_meta') or {})
        plan['idempotent_keys']['schema_meta'] = sorted(str(key) for key in (payload.get('schema_meta') or {}).keys())[:50]
    if section in {'all', 'schema_meta_ledger'} and isinstance(payload.get('schema_meta_ledger'), list):
        plan['sections'].append('schema_meta_ledger')
        plan['counts']['schema_meta_ledger'] = len(payload.get('schema_meta_ledger') or [])
    if section in {'all', 'panel_drift'} and isinstance(payload.get('panel_drift'), list):
        plan['sections'].append('panel_drift')
        plan['counts']['panel_drift'] = len(payload.get('panel_drift') or [])
    if section in {'all', 'runtime_markers'} and isinstance(payload.get('runtime_markers'), dict):
        plan['sections'].append('runtime_markers')
        plan['counts']['runtime_markers'] = len(payload.get('runtime_markers') or {})
        plan['idempotent_keys']['runtime_markers'] = sorted(str(key) for key in (payload.get('runtime_markers') or {}).keys())[:50]
    if section in {'all', 'bridge_comment_mirror'} and isinstance(payload.get('bridge_comment_mirror'), list):
        plan['sections'].append('bridge_comment_mirror')
        plan['counts']['bridge_comment_mirror'] = len(payload.get('bridge_comment_mirror') or [])
    if section in {'all', 'external_discussion_mirror'} and isinstance(payload.get('external_discussion_mirror'), list):
        plan['sections'].append('external_discussion_mirror')
        plan['counts']['external_discussion_mirror'] = len(payload.get('external_discussion_mirror') or [])
    if section in {'all', 'external_content_mirror'} and isinstance(payload.get('external_content_mirror'), list):
        plan['sections'].append('external_content_mirror')
        plan['counts']['external_content_mirror'] = len(payload.get('external_content_mirror') or [])
    for key in ('content_pack_meta', 'layout_spec_meta', 'runtime_markers_snapshot', 'build_metadata'):
        if section in {'all', key} and payload.get(key) is not None:
            plan['sections'].append(key)
            plan['counts'][key] = 1
            plan['idempotent_keys'][key] = [key]
    diagnostics_payload = {key: payload.get(key) for key in ('content_pack_meta', 'layout_spec_meta', 'runtime_markers_snapshot', 'build_metadata') if payload.get(key) is not None}
    if section in {'all', 'diagnostics'} and diagnostics_payload:
        plan['sections'].append('diagnostics')
        plan['counts']['diagnostics'] = len(diagnostics_payload)
        plan['idempotent_keys']['diagnostics'] = sorted(diagnostics_payload.keys())
    return plan


async def _apply_state_restore_payload(cog: CommunityCommands, payload: dict[str, Any], *, section: str, guild_id: str, actor_user_id: int) -> list[str]:
    backup_path = await cog.bot.capture_operational_backup(reason='state-restore', actor_user_id=actor_user_id, guild_id=int(guild_id) if str(guild_id or '').isdigit() else None)
    restored: list[str] = []
    skipped: dict[str, int] = {}
    if section in {'all', 'maintenance'} and isinstance(payload.get('maintenance_mode'), dict):
        cog.bot.maintenance_mode = dict(payload['maintenance_mode'])
        await cog.storage.database.set_key_value('maintenance_mode', cog.bot.maintenance_mode)
        restored.append('maintenance_mode')
    if section in {'all', 'forum_policies'} and isinstance(payload.get('runtime_forum_policy_overrides'), dict):
        cog.bot.runtime_forum_policy_overrides = dict(payload['runtime_forum_policy_overrides'])
        await cog.storage.database.set_key_value('runtime_forum_policy_overrides', cog.bot.runtime_forum_policy_overrides)
        restored.append('runtime_forum_policy_overrides')
    if section in {'all', 'panel_registry'} and isinstance(payload.get('panel_registry'), list):
        for row in payload['panel_registry'][:50]:
            panel_type = str(row.get('panel_type') or '').strip().lower()
            channel_id = str(row.get('channel_id') or '')
            message_id = str(row.get('message_id') or '')
            version = str(row.get('version') or '')
            if not panel_type or not channel_id or not message_id:
                continue
            await cog.community_store.upsert_panel_binding(guild_id=str(guild_id), panel_type=panel_type, channel_id=channel_id, message_id=message_id, version=version or 'restored', metadata=row.get('metadata_json') or {})
        restored.append('panel_registry')
    if section in {'all', 'layout_alias_bindings'} and isinstance(payload.get('layout_alias_bindings'), list):
        for row in payload['layout_alias_bindings'][:200]:
            alias = str(row.get('alias') or '').strip().lower()
            resource_type = str(row.get('resource_type') or '').strip().lower() or 'channel'
            discord_id = str(row.get('discord_id') or '').strip()
            if not alias or not discord_id:
                continue
            await cog.community_store.upsert_layout_alias_binding(guild_id=str(guild_id), alias=alias, resource_type=resource_type, discord_id=discord_id, metadata=row.get('metadata_json') or {})
        restored.append('layout_alias_bindings')
    if section in {'all', 'topics'} and isinstance(payload.get('topics'), list):
        applied = 0
        for row in payload['topics'][:250]:
            thread_id = str(row.get('thread_id') or '').strip()
            if not thread_id:
                continue
            await cog.community_store.register_forum_topic(
                thread_id=thread_id,
                guild_id=str(row.get('guild_id') or guild_id),
                forum_channel_id=str(row.get('forum_channel_id') or ''),
                topic_kind=str(row.get('topic_kind') or 'support'),
                owner_user_id=str(row.get('owner_user_id') or '') or None,
                title=str(row.get('title') or thread_id),
                tags=list(row.get('tags_json') or []),
                metadata=row.get('metadata_json') or {},
                auto_close_after_seconds=int(row.get('auto_close_after_seconds') or 0) or None,
            )
            await cog.community_store.update_forum_topic_state(thread_id=thread_id, status=str(row.get('status') or 'open'), tags=list(row.get('tags_json') or []), metadata=row.get('metadata_json') or {}, closed=bool(row.get('closed_at')))
            applied += 1
        restored.append('topics')
        skipped['topics_applied'] = applied
    if section in {'all', 'scheduled_jobs'} and isinstance(payload.get('scheduled_jobs'), list):
        created = 0
        skipped_count = 0
        for row in payload['scheduled_jobs'][:50]:
            job_type = str(row.get('job_type') or '').strip()
            run_at = str(row.get('run_at') or '').strip()
            if not job_type or not run_at:
                continue
            dedupe_key = str(row.get('dedupe_key') or '')
            if not dedupe_key:
                base = {
                    'job_type': job_type,
                    'guild_id': str(row.get('guild_id') or guild_id),
                    'channel_id': str(row.get('channel_id') or ''),
                    'run_at': run_at,
                    'payload_json': row.get('payload_json') or {},
                }
                dedupe_key = hashlib.sha256(json.dumps(base, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
            if await cog.community_store.find_scheduled_job_by_dedupe_key(dedupe_key):
                skipped_count += 1
                continue
            await cog.community_store.create_scheduled_job(job_type=job_type, guild_id=str(row.get('guild_id') or guild_id), channel_id=str(row.get('channel_id') or ''), payload=row.get('payload_json') or {}, run_at=run_at, created_by=str(actor_user_id), dedupe_key=dedupe_key)
            created += 1
        restored.append('scheduled_jobs')
        skipped['scheduled_jobs_existing'] = skipped_count
        skipped['scheduled_jobs_created'] = created
    if section in {'all', 'failed_bridge_events'} and isinstance(payload.get('failed_bridge_events'), list):
        replayed = 0
        skipped_count = 0
        for row in payload['failed_bridge_events'][:100]:
            event_kind = str(row.get('event_kind') or '').strip()
            destination = str(row.get('destination') or '').strip()
            if not event_kind or not destination:
                continue
            dedupe_key = str(row.get('dedupe_key') or '')
            if not dedupe_key:
                base = {'event_kind': event_kind, 'destination': destination, 'payload_json': row.get('payload_json') or {}}
                dedupe_key = hashlib.sha256(json.dumps(base, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
            if await cog.community_store.get_external_sync_event_by_dedupe_key(dedupe_key):
                skipped_count += 1
                continue
            await cog.community_store.queue_external_sync_event(event_kind=event_kind, destination=destination, payload=row.get('payload_json') or {}, dedupe_key=dedupe_key)
            replayed += 1
        restored.append('failed_bridge_events')
        skipped['failed_bridge_events_existing'] = skipped_count
        skipped['failed_bridge_events_replayed'] = replayed
    if section in {'all', 'bridge_destination_state'} and isinstance(payload.get('bridge_destination_state'), list):
        applied = 0
        for row in payload['bridge_destination_state'][:100]:
            destination = str(row.get('destination') or '').strip()
            if not destination:
                continue
            await cog.community_store.update_bridge_destination_state(destination=destination, circuit_state=str(row.get('circuit_state') or 'closed'), consecutive_failures=int(row.get('consecutive_failures') or 0), last_error=str(row.get('last_error') or ''), circuit_open_until=str(row.get('circuit_open_until') or '') or None, success=False, metadata=row.get('metadata_json') or {})
            applied += 1
        restored.append('bridge_destination_state')
        skipped['bridge_destination_state_applied'] = applied
    if section in {'all', 'schema_meta'} and isinstance(payload.get('schema_meta'), dict):
        applied = 0
        for key, value in list((payload.get('schema_meta') or {}).items())[:100]:
            await cog.community_store.upsert_schema_meta(key=str(key), value=str(value), source='state_restore')
            applied += 1
        restored.append('schema_meta')
        skipped['schema_meta_applied'] = applied
    if section in {'all', 'schema_meta_ledger'} and isinstance(payload.get('schema_meta_ledger'), list):
        applied = 0
        for row in payload['schema_meta_ledger'][:100]:
            key = str(row.get('key') or '').strip()
            if not key:
                continue
            await cog.community_store.record_schema_meta_ledger_entry(key=key, value=str(row.get('value') or ''), source=str(row.get('source') or 'state_restore'))
            applied += 1
        restored.append('schema_meta_ledger')
        skipped['schema_meta_ledger_applied'] = applied
    if section in {'all', 'panel_drift'} and isinstance(payload.get('panel_drift'), list):
        applied = 0
        for row in payload['panel_drift'][:100]:
            await cog.community_store.log_panel_drift(guild_id=str(row.get('guild_id') or guild_id), panel_type=str(row.get('panel_type') or 'unknown'), old_version=str(row.get('old_version') or ''), new_version=str(row.get('new_version') or ''), reason=str(row.get('reason') or 'snapshot_restore'), details=row.get('details_json') or {})
            applied += 1
        restored.append('panel_drift')
        skipped['panel_drift_applied'] = applied
    if section in {'all', 'runtime_markers'} and isinstance(payload.get('runtime_markers'), dict):
        applied = 0
        for key, value in list((payload.get('runtime_markers') or {}).items())[:100]:
            await cog.bot.set_runtime_marker(str(key), value)
            applied += 1
        restored.append('runtime_markers')
        skipped['runtime_markers_applied'] = applied
    if section in {'all', 'bridge_comment_mirror'} and isinstance(payload.get('bridge_comment_mirror'), list):
        applied = 0
        for row in payload['bridge_comment_mirror'][:250]:
            await cog.community_store.upsert_bridge_comment_mirror(thread_id=str(row.get('thread_id') or ''), source_platform=str(row.get('source_platform') or 'external'), external_comment_id=str(row.get('external_comment_id') or ''), discord_message_id=str(row.get('discord_message_id') or ''), metadata=row.get('metadata_json') or {})
            applied += 1
        restored.append('bridge_comment_mirror')
        skipped['bridge_comment_mirror_applied'] = applied
    if section in {'all', 'external_discussion_mirror'} and isinstance(payload.get('external_discussion_mirror'), list):
        applied = 0
        for row in payload['external_discussion_mirror'][:250]:
            await cog.community_store.upsert_external_discussion_mirror(source_platform=str(row.get('source_platform') or 'external'), external_topic_id=str(row.get('external_topic_id') or ''), topic_kind=str(row.get('topic_kind') or ''), discord_object_id=str(row.get('discord_object_id') or ''), channel_id=str(row.get('channel_id') or ''), metadata=row.get('metadata_json') or {})
            applied += 1
        restored.append('external_discussion_mirror')
        skipped['external_discussion_mirror_applied'] = applied
    if section in {'all', 'external_content_mirror'} and isinstance(payload.get('external_content_mirror'), list):
        applied = 0
        for row in payload['external_content_mirror'][:250]:
            await cog.community_store.upsert_external_content_mirror(source_platform=str(row.get('source_platform') or 'external'), content_kind=str(row.get('content_kind') or ''), external_content_id=str(row.get('external_content_id') or ''), discord_channel_id=str(row.get('discord_channel_id') or ''), discord_message_id=str(row.get('discord_message_id') or ''), metadata=row.get('metadata_json') or {})
            applied += 1
        restored.append('external_content_mirror')
        skipped['external_content_mirror_applied'] = applied
    for key in ('content_pack_meta', 'layout_spec_meta', 'runtime_markers_snapshot', 'build_metadata'):
        if section in {'all', key} and payload.get(key) is not None:
            await cog.bot.set_runtime_marker(f'snapshot:{key}', payload.get(key))
            restored.append(key)
            skipped[f'{key}_applied'] = 1
    if section in {'all', 'diagnostics'}:
        diagnostics = {key: payload.get(key) for key in ('content_pack_meta', 'layout_spec_meta', 'runtime_markers_snapshot', 'build_metadata') if payload.get(key) is not None}
        applied = 0
        for key, value in diagnostics.items():
            await cog.bot.set_runtime_marker(f'snapshot:{key}', value)
            applied += 1
        if diagnostics:
            restored.append('diagnostics')
            skipped['diagnostics_applied'] = applied
    await cog.bot.record_audit(action='state_restore', actor_user_id=actor_user_id, target_user_id=None, status='success', payload={'section': section, 'restored': restored, 'skipped': skipped, 'backup_path': str(backup_path) if backup_path else '', 'approved': True})
    return restored


async def _create_external_forum_topic(bot: "NMDiscordBot", guild: discord.Guild, *, topic_kind: str, title: str, body: str, actor_user_id: str, actor_name: str, metadata: dict[str, Any] | None = None) -> discord.Thread | None:
    forum = _resolve_forum_for_topic(bot, guild, topic_kind)
    if forum is None:
        return None
    metadata = dict(metadata or {})
    tags = await bot._ensure_forum_tags(forum, bot._forum_tag_names_for_kind(topic_kind, 'open'))
    content = body[:4000]
    thread = await forum.create_thread(name=title[:100], content=content, applied_tags=tags[:5] if tags else None)
    await bot.community_store.register_forum_topic(thread_id=str(thread.id), guild_id=str(guild.id), forum_channel_id=str(forum.id), topic_kind=topic_kind, owner_user_id=actor_user_id, title=title[:250], tags=[tag.name for tag in tags[:5]] if tags else [bot.settings.forum_tag_status_open_name], metadata={'created_via': 'external-bridge', 'actor_name': actor_name, **metadata}, auto_close_after_seconds=bot.settings.forum_auto_close_inactive_hours * 3600)
    return thread


@commands.command(name="ping")
async def prefix_ping(ctx: commands.Context[Any]) -> None:
    latency_ms = int(ctx.bot.latency * 1000)
    await ctx.reply(f"Бот отвечает. Задержка шлюза: {latency_ms} мс")


def build_status_embed(server_name: str, payload: dict[str, Any]) -> discord.Embed:
    online = bool(_pick(payload, ["online", "is_online", "server_online"], default=False))
    players_online = _pick(payload, ["players_online", "online_players", "onlineCount"], default="?")
    players_max = _pick(payload, ["players_max", "max_players", "maxCount", "max"], default="?")
    version = _pick(payload, ["version", "minecraft_version"], default="?")
    motd = _pick(payload, ["motd", "description"], default="—")

    embed = discord.Embed(title=f"Статус {server_name}", color=EMBED_COLOR if online else ERROR_COLOR)
    embed.add_field(name="Состояние", value="Онлайн" if online else "Оффлайн", inline=True)
    embed.add_field(name="Игроки", value=f"{players_online}/{players_max}", inline=True)
    embed.add_field(name="Версия", value=str(version), inline=True)
    embed.add_field(name="MOTD", value=str(motd), inline=False)
    return embed


def _error_embed(title: str, description: str) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=ERROR_COLOR)


def _pick(payload: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return default


_NM_DISCORD_BOT_METHODS = bind_bot_extensions(NMDiscordBot, globals())
