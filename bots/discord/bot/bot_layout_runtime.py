from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any

import discord

from .server_layout import (
    ensure_server_layout_file,
    expected_forum_tags,
    find_layout_channel,
    find_layout_role,
    forum_aliases_by_kind,
    load_server_layout,
    validate_server_layout,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            parsed = datetime.strptime(raw, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _format_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _resolve_layout_subject(guild: discord.Guild, spec: dict[str, Any], subject: str) -> discord.Role | None:
    subject_norm = str(subject or '').strip().lower()
    if subject_norm in {'everyone', '@everyone', 'default'}:
        return guild.default_role
    role_entry = find_layout_role(spec, subject_norm)
    role_name = str((role_entry or {}).get('name') or subject or '').strip().lower()
    for role in guild.roles:
        if str(role.name).strip().lower() == role_name:
            return role
    return None


def _build_overwrite(allow: list[str] | tuple[str, ...] | None = None, deny: list[str] | tuple[str, ...] | None = None) -> discord.PermissionOverwrite:
    overwrite = discord.PermissionOverwrite()
    for name in allow or []:
        if hasattr(overwrite, str(name)):
            setattr(overwrite, str(name), True)
    for name in deny or []:
        if hasattr(overwrite, str(name)):
            setattr(overwrite, str(name), False)
    return overwrite


def _overwrite_matches(existing: discord.PermissionOverwrite, *, allow: list[str] | tuple[str, ...] | None = None, deny: list[str] | tuple[str, ...] | None = None) -> bool:
    for name in allow or []:
        if getattr(existing, str(name), None) is not True:
            return False
    for name in deny or []:
        if getattr(existing, str(name), None) is not False:
            return False
    return True


def _role_permissions_match(role: discord.Role, names: list[str] | tuple[str, ...] | None) -> bool:
    for name in names or []:
        if not getattr(role.permissions, str(name), False):
            return False
    return True


def _layout_permission_entries(*entries: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, tuple[str, ...], tuple[str, ...]]] = set()
    for entry in entries:
        for item in entry.get('permission_overrides') or []:
            if not isinstance(item, dict):
                continue
            subject = str(item.get('subject') or '').strip().lower()
            allow = tuple(str(x).strip() for x in (item.get('allow') or []) if str(x).strip())
            deny = tuple(str(x).strip() for x in (item.get('deny') or []) if str(x).strip())
            if not subject:
                continue
            key = (subject, allow, deny)
            if key in seen:
                continue
            seen.add(key)
            result.append({'subject': subject, 'allow': list(allow), 'deny': list(deny)})
    return result


def _effective_layout_permission_entries(category_entry: dict[str, Any], channel_entry: dict[str, Any]) -> list[dict[str, Any]]:
    entries = _layout_permission_entries(category_entry, channel_entry)
    read_only = bool(channel_entry.get('read_only_public'))
    staff_only = bool(channel_entry.get('staff_only') or category_entry.get('staff_only'))
    channel_type = str(channel_entry.get('type') or 'text').strip().lower()
    if read_only:
        deny = ['send_messages']
        if channel_type == 'forum':
            deny.extend(['create_public_threads', 'send_messages_in_threads'])
        elif channel_type in {'voice', 'stage'}:
            deny.append('speak')
        entries = _layout_permission_entries({'permission_overrides': [{'subject': 'everyone', 'deny': deny}]}, {'permission_overrides': entries})
    if staff_only:
        entries = _layout_permission_entries(
            {'permission_overrides': [{'subject': 'everyone', 'deny': ['view_channel', 'connect', 'speak', 'send_messages', 'send_messages_in_threads', 'create_public_threads']}]},
            {'permission_overrides': entries},
        )
    return entries


def _layout_settings_alias_map(bot: "NMDiscordBot") -> dict[str, int | None]:
    return {
        'start_here': bot.settings.discord_start_here_channel_id,
        'rules': bot.settings.discord_rules_channel_id,
        'announcements': bot.settings.discord_announcements_channel_id,
        'devlog': bot.settings.discord_devlog_channel_id,
        'roles_and_access': bot.settings.discord_roles_channel_id,
        'faq_discord': bot.settings.discord_faq_channel_id,
        'world_signals': bot.settings.discord_world_signals_channel_id,
        'suggestions_forum': bot.settings.discord_forum_suggestions_channel_id,
        'bug_reports_forum': bot.settings.discord_forum_bug_reports_channel_id,
        'guild_recruitment_forum': bot.settings.discord_forum_guild_recruitment_channel_id,
        'help_forum': bot.settings.discord_forum_help_channel_id,
        'launcher_tech_forum': bot.settings.discord_forum_launcher_and_tech_channel_id,
        'account_help_forum': bot.settings.discord_forum_account_help_channel_id,
        'appeals_forum': bot.settings.discord_forum_appeals_channel_id,
        'reports': bot.settings.discord_reports_channel_id,
        'bot_logs': bot.settings.discord_bot_logs_channel_id,
        'stage_nevermine': bot.settings.discord_stage_channel_id,
    }


def _resolve_layout_channel(bot: "NMDiscordBot", guild: discord.Guild, alias: str) -> discord.abc.GuildChannel | None:
    alias_norm = str(alias or '').strip().lower()
    mapped_id = _layout_settings_alias_map(bot).get(alias_norm)
    if mapped_id:
        channel = bot.get_channel(mapped_id)
        if isinstance(channel, discord.abc.GuildChannel):
            return channel
    cached = (getattr(bot, 'layout_alias_bindings_cache', {}) or {}).get(('channel', alias_norm))
    if str(cached or '').isdigit():
        channel = bot.get_channel(int(str(cached)))
        if isinstance(channel, discord.abc.GuildChannel):
            return channel
    spec = load_server_layout(ensure_server_layout_file())
    entry = find_layout_channel(spec, alias)
    if entry is None:
        return None
    category_name = str(entry.get('category_name') or '').strip().lower()
    channel_name = str(entry.get('name') or '').strip().lower()
    expected_type = str(entry.get('type') or 'text').strip().lower()
    for channel in guild.channels:
        if str(channel.name).strip().lower() != channel_name:
            continue
        parent_name = str(getattr(channel.category, 'name', '') or '').strip().lower()
        if category_name and parent_name != category_name:
            continue
        if expected_type == 'forum' and not isinstance(channel, discord.ForumChannel):
            continue
        if expected_type == 'voice' and not isinstance(channel, discord.VoiceChannel):
            continue
        if expected_type == 'stage' and not isinstance(channel, discord.StageChannel):
            continue
        if expected_type == 'text' and not isinstance(channel, discord.TextChannel):
            continue
        return channel
    if str(cached or '').isdigit():
        channel = bot.get_channel(int(str(cached)))
        if isinstance(channel, discord.abc.GuildChannel):
            return channel
    return None


def _resolve_forum_for_topic(bot: "NMDiscordBot", guild: discord.Guild, topic_kind: str) -> discord.ForumChannel | None:
    direct_map = {
        'support': bot.settings.discord_forum_help_channel_id,
        'bug': bot.settings.discord_forum_bug_reports_channel_id,
        'suggestion': bot.settings.discord_forum_suggestions_channel_id,
        'appeal': bot.settings.discord_forum_appeals_channel_id,
        'guild_recruitment': bot.settings.discord_forum_guild_recruitment_channel_id,
        'chronicle': None,
        'lore_discussion': None,
    }
    direct_id = direct_map.get(str(topic_kind or '').strip().lower())
    if direct_id:
        channel = bot.get_channel(direct_id)
        if isinstance(channel, discord.ForumChannel):
            return channel
    spec = load_server_layout(ensure_server_layout_file())
    for alias in forum_aliases_by_kind(spec).get(str(topic_kind or '').strip().lower(), []):
        channel = _resolve_layout_channel(bot, guild, alias)
        if isinstance(channel, discord.ForumChannel):
            return channel
    return None


async def _collect_layout_drift(bot: "NMDiscordBot", guild: discord.Guild) -> dict[str, Any]:
    spec = load_server_layout(ensure_server_layout_file())
    drift: dict[str, Any] = {
        'layout_issues': validate_server_layout(spec),
        'missing_categories': [],
        'missing_roles': [],
        'missing_channels': [],
        'readonly_issues': [],
        'forum_tag_issues': [],
        'panel_binding_issues': [],
        'channel_type_mismatches': [],
        'channel_category_mismatches': [],
        'channel_name_mismatches': [],
        'extra_channels': [],
        'extra_roles': [],
        'role_color_mismatches': [],
        'role_position_mismatches': [],
        'role_permission_mismatches': [],
        'permission_matrix_issues': [],
        'staff_visibility_issues': [],
    }
    existing_categories = {str(c.name).strip().lower(): c for c in guild.categories}
    existing_roles = {str(r.name).strip().lower(): r for r in guild.roles}
    expected_role_names = set()
    category_entries = {str(cat.get('alias') or '').strip().lower(): cat for cat in (spec.get('categories') or []) if isinstance(cat, dict)}
    for role in spec.get('roles') or []:
        if not isinstance(role, dict):
            continue
        name = str(role.get('name') or '').strip()
        alias = str(role.get('alias') or '').strip().lower()
        if name:
            expected_role_names.add(name.lower())
        existing = existing_roles.get(name.lower()) if name else None
        if existing is None:
            drift['missing_roles'].append({'name': name, 'alias': alias})
            continue
        color = str(role.get('color') or '').strip()
        if color:
            expected_color = int(color.lstrip('#'), 16)
            if int(getattr(existing.color, 'value', 0) or 0) != expected_color:
                drift['role_color_mismatches'].append({'alias': alias, 'name': name, 'expected_color': color, 'actual_color': f"#{int(getattr(existing.color, 'value', 0) or 0):06X}"})
        if role.get('position') is not None:
            try:
                expected_position = int(role.get('position'))
                if int(existing.position) != expected_position:
                    drift['role_position_mismatches'].append({'alias': alias, 'name': name, 'expected_position': expected_position, 'actual_position': int(existing.position)})
            except Exception:
                pass
        desired_permissions = [str(x).strip() for x in (role.get('permissions') or []) if str(x).strip()]
        if desired_permissions and not _role_permissions_match(existing, desired_permissions):
            actual_permissions = sorted(name for name, enabled in existing.permissions if enabled)
            drift['role_permission_mismatches'].append({'alias': alias, 'name': name, 'expected_permissions': desired_permissions, 'actual_permissions': actual_permissions})
    manage_extras = any(str(x.get('managed', True)).lower() != 'false' for x in spec.get('roles') or [] if isinstance(x, dict))
    for role in guild.roles:
        if role.managed or role.is_default():
            continue
        if str(role.name).strip().lower() not in expected_role_names and manage_extras:
            if str(role.name).strip().lower() in {'@everyone'}:
                continue
            drift['extra_roles'].append({'name': role.name, 'id': role.id})
    expected_channel_names = set()
    managed_category_names = set()
    for category in spec.get('categories') or []:
        if not isinstance(category, dict):
            continue
        category_name = str(category.get('name') or '').strip()
        category_alias = str(category.get('alias') or '').strip().lower()
        managed_category_names.add(category_name.lower())
        if category_name and category_name.lower() not in existing_categories:
            drift['missing_categories'].append({'name': category_name, 'alias': category_alias})
        for channel in category.get('channels') or []:
            if not isinstance(channel, dict):
                continue
            alias = str(channel.get('alias') or '').strip().lower()
            expected_name = str(channel.get('name') or '').strip()
            ctype = str(channel.get('type') or 'text').strip().lower()
            expected_channel_names.add((category_name.lower(), expected_name.lower()))
            resolved = _resolve_layout_channel(bot, guild, alias)
            if resolved is None:
                drift['missing_channels'].append({'alias': alias, 'name': expected_name, 'type': ctype, 'category_name': category_name})
                continue
            actual_parent = str(getattr(resolved.category, 'name', '') or '').strip().lower()
            if category_name and actual_parent != category_name.lower():
                drift['channel_category_mismatches'].append({'alias': alias, 'channel_id': resolved.id, 'expected_category': category_name, 'actual_category': getattr(resolved.category, 'name', '') or ''})
            if str(resolved.name).strip().lower() != expected_name.lower():
                drift['channel_name_mismatches'].append({'alias': alias, 'channel_id': resolved.id, 'expected_name': expected_name, 'actual_name': resolved.name})
            type_ok = {
                'forum': isinstance(resolved, discord.ForumChannel),
                'voice': isinstance(resolved, discord.VoiceChannel),
                'stage': isinstance(resolved, discord.StageChannel),
                'text': isinstance(resolved, discord.TextChannel),
            }.get(ctype, True)
            if not type_ok:
                drift['channel_type_mismatches'].append({'alias': alias, 'channel_id': resolved.id, 'expected_type': ctype, 'actual_type': type(resolved).__name__})
            if bool(channel.get('read_only_public')) and isinstance(resolved, discord.TextChannel):
                perms = resolved.permissions_for(guild.default_role)
                if getattr(perms, 'send_messages', False):
                    drift['readonly_issues'].append({'alias': alias, 'channel_id': resolved.id, 'name': resolved.name})
            if bool(channel.get('staff_only') or category.get('staff_only')):
                perms = resolved.permissions_for(guild.default_role)
                if getattr(perms, 'view_channel', False):
                    drift['staff_visibility_issues'].append({'alias': alias, 'channel_id': resolved.id, 'name': resolved.name})
            if isinstance(resolved, discord.ForumChannel):
                expected = expected_forum_tags(spec).get(alias, [])
                if not expected:
                    expected = bot._forum_tag_names_for_kind(str(channel.get('topic_kind') or 'support'), 'open')
                existing = {tag.name.strip().lower() for tag in resolved.available_tags}
                missing = [name for name in expected if name and name.strip().lower() not in existing]
                if missing:
                    drift['forum_tag_issues'].append({'alias': alias, 'channel_id': resolved.id, 'missing_tags': missing})
            for entry in _effective_layout_permission_entries(category, channel):
                subject = _resolve_layout_subject(guild, spec, str(entry.get('subject') or ''))
                if subject is None:
                    drift['permission_matrix_issues'].append({'alias': alias, 'channel_id': resolved.id, 'subject': entry.get('subject') or '', 'missing_subject': True, 'allow': list(entry.get('allow') or []), 'deny': list(entry.get('deny') or [])})
                    continue
                current = resolved.overwrites_for(subject)
                if not _overwrite_matches(current, allow=list(entry.get('allow') or []), deny=list(entry.get('deny') or [])):
                    drift['permission_matrix_issues'].append({'alias': alias, 'channel_id': resolved.id, 'subject': entry.get('subject') or '', 'allow': list(entry.get('allow') or []), 'deny': list(entry.get('deny') or []), 'actual': current.pair()[0].value if hasattr(current, 'pair') else ''})
    for channel in guild.channels:
        parent_name = str(getattr(channel.category, 'name', '') or '').strip().lower()
        key = (parent_name, str(channel.name).strip().lower())
        if parent_name in managed_category_names and key not in expected_channel_names:
            drift['extra_channels'].append({'name': channel.name, 'id': channel.id, 'category_name': getattr(channel.category, 'name', '') or ''})
    guild_ref = str(guild.id)
    for binding in await bot.community_store.list_panel_bindings(guild_id=guild_ref):
        channel = bot.get_channel(int(binding.get('channel_id') or 0)) if str(binding.get('channel_id') or '').isdigit() else None
        if channel is None:
            drift['panel_binding_issues'].append({'panel_type': binding.get('panel_type') or '', 'channel_id': binding.get('channel_id') or ''})
    return drift


def _summarize_layout_drift(drift: dict[str, Any], scope: str) -> list[str]:
    lines: list[str] = []
    mapping = {
        'missing_roles': 'отсутствуют роли',
        'missing_categories': 'отсутствуют категории',
        'missing_channels': 'отсутствуют каналы',
        'readonly_issues': 'нарушен read-only режим',
        'forum_tag_issues': 'неполные forum tags',
        'panel_binding_issues': 'битые panel bindings',
        'channel_type_mismatches': 'расхождения типов каналов',
        'channel_category_mismatches': 'каналы в неверных категориях',
        'channel_name_mismatches': 'расхождения имён каналов',
        'extra_channels': 'лишние каналы в managed categories',
        'extra_roles': 'лишние роли',
        'role_color_mismatches': 'расхождения цветов ролей',
        'role_position_mismatches': 'расхождения позиций ролей',
        'role_permission_mismatches': 'расхождения прав ролей',
        'permission_matrix_issues': 'расхождения permission matrix',
        'staff_visibility_issues': 'staff-only каналы видны всем',
    }
    for key, label in mapping.items():
        count = len(drift.get(key) or [])
        if count:
            lines.append(f'{label}: {count}')
    if drift.get('layout_issues'):
        lines.append(f'ошибки layout spec: {len(drift.get("layout_issues") or [])}')
    return lines


async def _apply_layout_repair(bot: "NMDiscordBot", guild: discord.Guild, drift: dict[str, Any], scope: str) -> list[str]:
    fixes: list[str] = []
    spec = load_server_layout(ensure_server_layout_file())
    category_map = {str(c.name).strip().lower(): c for c in guild.categories}
    role_map = {str(r.name).strip().lower(): r for r in guild.roles}
    guild_ref = str(guild.id)

    def _role_permission_kwargs(names: list[str] | None) -> dict[str, bool]:
        return {str(name).strip(): True for name in (names or []) if str(name).strip()}

    async def _sync_channel_permissions(channel: discord.abc.GuildChannel, *, category_entry: dict[str, Any], channel_entry: dict[str, Any]) -> None:
        for entry in _effective_layout_permission_entries(category_entry, channel_entry):
            subject_alias = str(entry.get('subject') or '').strip().lower()
            subject = _resolve_layout_subject(guild, spec, subject_alias)
            if subject is None:
                continue
            overwrite = _build_overwrite(list(entry.get('allow') or []), list(entry.get('deny') or []))
            with contextlib.suppress(Exception):
                await channel.set_permissions(subject, overwrite=overwrite, reason='NMDiscordBot layout_repair permission matrix')
                fixes.append(f'permission matrix обновлена: {channel.name} → {subject.name}')

    async def _create_expected_channel(entry: dict[str, Any], *, category_name: str | None = None, replacement: bool = False) -> discord.abc.GuildChannel | None:
        name = str(entry.get('name') or '').strip()
        alias = str(entry.get('alias') or '').strip().lower()
        ctype = str(entry.get('type') or 'text').strip().lower()
        category = category_map.get(str(category_name or entry.get('category_name') or '').strip().lower())
        category_entry = next((item for item in (spec.get('categories') or []) if isinstance(item, dict) and str(item.get('name') or '').strip().lower() == str(category_name or entry.get('category_name') or '').strip().lower()), {})
        overwrite_map: dict[Any, discord.PermissionOverwrite] = {}
        for perm_entry in _effective_layout_permission_entries(category_entry if isinstance(category_entry, dict) else {}, entry):
            subject = _resolve_layout_subject(guild, spec, str(perm_entry.get('subject') or ''))
            if subject is None:
                continue
            overwrite_map[subject] = _build_overwrite(list(perm_entry.get('allow') or []), list(perm_entry.get('deny') or []))
        overwrites = overwrite_map or None
        if not name:
            return None
        reason = 'NMDiscordBot layout_repair replacement' if replacement else 'NMDiscordBot layout_repair'
        if ctype == 'forum':
            created = await guild.create_forum(name=name, category=category, overwrites=overwrites, reason=reason) if category is not None else await guild.create_forum(name=name, overwrites=overwrites, reason=reason)
        elif ctype == 'voice':
            created = await guild.create_voice_channel(name=name, category=category, overwrites=overwrites, reason=reason) if category is not None else await guild.create_voice_channel(name=name, overwrites=overwrites, reason=reason)
        elif ctype == 'stage':
            created = await guild.create_stage_channel(name=name, category=category, overwrites=overwrites, reason=reason) if category is not None else await guild.create_stage_channel(name=name, overwrites=overwrites, reason=reason)
        else:
            created = await guild.create_text_channel(name=name, category=category, overwrites=overwrites, reason=reason) if category is not None else await guild.create_text_channel(name=name, overwrites=overwrites, reason=reason)
        await bot.remember_layout_alias_binding(guild_ref, alias=alias, resource_type='channel', discord_id=created.id, metadata={'name': created.name, 'type': ctype})
        if isinstance(created, discord.ForumChannel):
            tag_names = list(expected_forum_tags(spec).get(alias, [])) or bot._forum_tag_names_for_kind(str(entry.get('topic_kind') or 'support'), 'open')
            if tag_names:
                await bot._ensure_forum_tags(created, tag_names)
        with contextlib.suppress(Exception):
            await _sync_channel_permissions(created, category_entry=category_entry if isinstance(category_entry, dict) else {}, channel_entry=entry)
        fixes.append(f'канал {"заменён" if replacement else "создан"}: {name}')
        return created

    if scope in {'all', 'roles', 'drift'}:
        role_reorders: list[discord.Role] = []
        for item in spec.get('roles') or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get('name') or '').strip()
            alias = str(item.get('alias') or '').strip().lower()
            color = str(item.get('color') or '').strip()
            position = int(item.get('position') or 0) if item.get('position') is not None else None
            desired_permissions = [str(x).strip() for x in (item.get('permissions') or []) if str(x).strip()]
            role = role_map.get(name.lower()) if name else None
            role_color = discord.Colour(int(color.lstrip('#'), 16)) if color else None
            if role is None and name:
                role = await guild.create_role(name=name, color=role_color or discord.Colour.default(), permissions=discord.Permissions(**_role_permission_kwargs(desired_permissions)) if desired_permissions else discord.Permissions.none(), reason='NMDiscordBot layout_repair')
                role_map[name.lower()] = role
                fixes.append(f'роль создана: {name}')
            if role is not None:
                await bot.remember_layout_alias_binding(guild_ref, alias=alias, resource_type='role', discord_id=role.id, metadata={'name': role.name})
                updates = {}
                if role_color is not None and int(role.color.value) != int(role_color.value):
                    updates['color'] = role_color
                if desired_permissions:
                    perm_dict = {key: False for key in role.permissions.to_dict().keys()}
                    for perm_name in desired_permissions:
                        perm_dict[str(perm_name).strip()] = True
                    desired_perm_obj = discord.Permissions(**perm_dict)
                    if role.permissions.value != desired_perm_obj.value:
                        updates['permissions'] = desired_perm_obj
                if updates:
                    await role.edit(reason='NMDiscordBot layout_repair', **updates)
                    fixes.append(f'роль обновлена: {name}')
                if position is not None:
                    role_reorders.append(role)
        desired_positions = {str(item.get('name') or '').strip().lower(): int(item.get('position') or 0) for item in spec.get('roles') or [] if isinstance(item, dict) and item.get('position') is not None}
        if desired_positions:
            ordered = sorted([r for r in role_reorders if str(r.name).strip().lower() in desired_positions], key=lambda r: desired_positions.get(str(r.name).strip().lower(), 0))
            for role in ordered:
                expected = desired_positions.get(str(role.name).strip().lower(), role.position)
                if role.position != expected:
                    with contextlib.suppress(Exception):
                        await role.edit(position=expected, reason='NMDiscordBot layout_repair position')
                        fixes.append(f'позиция роли обновлена: {role.name}')

    if scope in {'all', 'channels', 'forums', 'tags', 'readonly', 'drift', 'only-missing-tags', 'only-permission-check'}:
        for item in drift.get('missing_categories') or []:
            name = str(item.get('name') or '').strip()
            alias = str(item.get('alias') or '').strip().lower()
            if name and name.lower() not in category_map:
                category = await guild.create_category(name=name, reason='NMDiscordBot layout_repair')
                category_map[name.lower()] = category
                fixes.append(f'категория создана: {name}')
                if alias:
                    await bot.remember_layout_alias_binding(guild_ref, alias=alias, resource_type='category', discord_id=category.id, metadata={'name': category.name})
        for item in drift.get('missing_channels') or []:
            entry = find_layout_channel(spec, str(item.get('alias') or '')) or {}
            if entry:
                await _create_expected_channel(entry, category_name=str(item.get('category_name') or entry.get('category_name') or ''))
        for item in drift.get('channel_type_mismatches') or []:
            entry = find_layout_channel(spec, str(item.get('alias') or '')) or {}
            if entry:
                replacement = await _create_expected_channel(entry, category_name=str(entry.get('category_name') or ''), replacement=True)
                legacy = bot.get_channel(int(item.get('channel_id') or 0))
                if replacement is not None and legacy is not None and scope in {'all', 'strict', 'drift'}:
                    with contextlib.suppress(Exception):
                        old_name = str(getattr(legacy, 'name', 'channel') or 'channel')
                        await legacy.edit(name=f'legacy-{old_name}'[:100], reason='NMDiscordBot layout_repair legacy channel after replacement')
                        await bot.community_store.upsert_legacy_layout_resource(guild_id=guild_ref, resource_type='channel', discord_id=str(getattr(legacy, 'id', '') or ''), resource_name=old_name, review_after=_format_dt(_utc_now() + timedelta(days=3)), delete_after=_format_dt(_utc_now() + timedelta(days=14)), metadata={'reason': 'replacement', 'alias': str(item.get('alias') or '')})
                        fixes.append(f'старый канал помечен legacy: {old_name}')
        for item in drift.get('readonly_issues') or []:
            channel = bot.get_channel(int(item.get('channel_id') or 0))
            if isinstance(channel, discord.TextChannel):
                overwrite = channel.overwrites_for(guild.default_role)
                overwrite.send_messages = False
                await channel.set_permissions(guild.default_role, overwrite=overwrite, reason='NMDiscordBot layout_repair read-only')
                fixes.append(f'read-only применён: {channel.name}')
        for item in drift.get('staff_visibility_issues') or []:
            channel = bot.get_channel(int(item.get('channel_id') or 0))
            if isinstance(channel, discord.abc.GuildChannel):
                overwrite = channel.overwrites_for(guild.default_role)
                overwrite.view_channel = False
                with contextlib.suppress(Exception):
                    await channel.set_permissions(guild.default_role, overwrite=overwrite, reason='NMDiscordBot layout_repair staff-only')
                    fixes.append(f'staff visibility исправлен: {channel.name}')
        for item in drift.get('forum_tag_issues') or []:
            channel = bot.get_channel(int(item.get('channel_id') or 0))
            if isinstance(channel, discord.ForumChannel):
                await bot._ensure_forum_tags(channel, list(item.get('missing_tags') or []))
                fixes.append(f'forum tags дополнены: {channel.name}')
        for item in drift.get('channel_name_mismatches') or []:
            channel = bot.get_channel(int(item.get('channel_id') or 0))
            expected_name = str(item.get('expected_name') or '').strip()
            if channel is not None and expected_name:
                with contextlib.suppress(Exception):
                    await channel.edit(name=expected_name, reason='NMDiscordBot layout_repair rename')
                    fixes.append(f'канал переименован: {expected_name}')
        for item in drift.get('channel_category_mismatches') or []:
            channel = bot.get_channel(int(item.get('channel_id') or 0))
            category = category_map.get(str(item.get('expected_category') or '').strip().lower())
            if channel is not None and category is not None:
                with contextlib.suppress(Exception):
                    await channel.edit(category=category, reason='NMDiscordBot layout_repair category')
                    fixes.append(f'канал перемещён в категорию: {channel.name}')
        if scope in {'all', 'strict', 'drift'}:
            for item in drift.get('extra_channels') or []:
                channel = bot.get_channel(int(item.get('id') or 0))
                if channel is None:
                    continue
                current_name = str(getattr(channel, 'name', '') or 'channel')
                if current_name.startswith('legacy-'):
                    continue
                with contextlib.suppress(Exception):
                    await channel.edit(name=f'legacy-{current_name}'[:100], reason='NMDiscordBot layout_repair strict extra channel')
                    await bot.community_store.upsert_legacy_layout_resource(guild_id=guild_ref, resource_type='channel', discord_id=str(getattr(channel, 'id', '') or ''), resource_name=current_name, review_after=_format_dt(_utc_now() + timedelta(days=3)), delete_after=_format_dt(_utc_now() + timedelta(days=14)), metadata={'reason': 'extra_channel'})
                    fixes.append(f'лишний канал помечен legacy: {current_name}')
            for item in drift.get('extra_roles') or []:
                role = guild.get_role(int(item.get('id') or 0))
                if role is None or role.managed:
                    continue
                current_name = str(role.name or 'role')
                if current_name.startswith('legacy-'):
                    continue
                with contextlib.suppress(Exception):
                    await role.edit(name=f'legacy-{current_name}'[:100], reason='NMDiscordBot layout_repair strict extra role')
                    await bot.community_store.upsert_legacy_layout_resource(guild_id=guild_ref, resource_type='role', discord_id=str(getattr(role, 'id', '') or ''), resource_name=current_name, review_after=_format_dt(_utc_now() + timedelta(days=3)), delete_after=_format_dt(_utc_now() + timedelta(days=14)), metadata={'reason': 'extra_role'})
                    fixes.append(f'лишняя роль помечена legacy: {current_name}')
        processed_aliases: set[str] = set()
        for item in drift.get('permission_matrix_issues') or []:
            alias = str(item.get('alias') or '').strip().lower()
            if not alias or alias in processed_aliases:
                continue
            processed_aliases.add(alias)
            channel = bot.get_channel(int(item.get('channel_id') or 0))
            if not isinstance(channel, discord.abc.GuildChannel):
                continue
            entry = find_layout_channel(spec, alias) or {}
            category_entry = next((cat for cat in (spec.get('categories') or []) if isinstance(cat, dict) and any(isinstance(ch, dict) and str(ch.get('alias') or '').strip().lower() == alias for ch in (cat.get('channels') or []))), {})
            if entry:
                with contextlib.suppress(Exception):
                    await _sync_channel_permissions(channel, category_entry=category_entry if isinstance(category_entry, dict) else {}, channel_entry=entry)
    await bot.refresh_layout_alias_cache(guild_ref)
    return fixes


async def _apply_legacy_layout_cleanup(bot: "NMDiscordBot", guild: discord.Guild, *, limit: int = 20) -> list[str]:
    actions: list[str] = []
    rows = await bot.community_store.list_legacy_layout_resources(guild_id=str(guild.id), due_only=False, limit=limit)
    now = _utc_now()
    for row in rows:
        delete_after = _parse_datetime(str(row.get('delete_after') or ''))
        if delete_after is None or delete_after > now:
            continue
        resource_type = str(row.get('resource_type') or '').strip().lower()
        discord_id = str(row.get('discord_id') or '')
        if resource_type == 'channel' and discord_id.isdigit():
            channel = bot.get_channel(int(discord_id))
            if channel is not None and str(getattr(channel, 'name', '') or '').startswith('legacy-'):
                with contextlib.suppress(Exception):
                    await channel.delete(reason='NMDiscordBot legacy layout cleanup')
                    await bot.community_store.update_legacy_layout_resource_status(guild_id=str(guild.id), resource_type='channel', discord_id=discord_id, status='deleted')
                    actions.append(f'удалён legacy-канал: {row.get("resource_name") or discord_id}')
        if resource_type == 'role' and discord_id.isdigit():
            role = guild.get_role(int(discord_id))
            if role is not None and not role.managed and str(role.name or '').startswith('legacy-'):
                with contextlib.suppress(Exception):
                    await role.delete(reason='NMDiscordBot legacy layout cleanup')
                    await bot.community_store.update_legacy_layout_resource_status(guild_id=str(guild.id), resource_type='role', discord_id=discord_id, status='deleted')
                    actions.append(f'удалена legacy-роль: {row.get("resource_name") or discord_id}')
    return actions

