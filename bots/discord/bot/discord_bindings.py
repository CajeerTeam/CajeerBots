from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import discord

from .config import Settings
from .server_layout import load_server_layout

ROOT_DIR = Path(__file__).resolve().parent.parent

CHANNEL_ENV_BY_ALIAS = {
    'announcements': 'DISCORD_ANNOUNCEMENTS_CHANNEL_ID',
    'events_and_seasons': 'DISCORD_EVENTS_CHANNEL_ID',
    'staff_briefing': 'DISCORD_AUDIT_CHANNEL_ID',
    'bot_logs': 'DISCORD_BOT_LOGS_CHANNEL_ID',
    'reports': 'DISCORD_REPORTS_CHANNEL_ID',
    'start_here': 'DISCORD_START_HERE_CHANNEL_ID',
    'rules': 'DISCORD_RULES_CHANNEL_ID',
    'roles_and_access': 'DISCORD_ROLES_AND_ACCESS_CHANNEL_ID',
    'faq_discord': 'DISCORD_FAQ_CHANNEL_ID',
    'devlog': 'DISCORD_DEVLOG_CHANNEL_ID',
    'world_signals': 'DISCORD_WORLD_SIGNALS_CHANNEL_ID',
    'stage_nevermine': 'DISCORD_STAGE_CHANNEL_ID',
    'suggestions_forum': 'DISCORD_FORUM_SUGGESTIONS_CHANNEL_ID',
    'bug_reports_forum': 'DISCORD_FORUM_BUG_REPORTS_CHANNEL_ID',
    'guild_recruitment_forum': 'DISCORD_FORUM_GUILD_RECRUITMENT_CHANNEL_ID',
    'help_forum': 'DISCORD_FORUM_HELP_CHANNEL_ID',
    'launcher_tech_forum': 'DISCORD_FORUM_LAUNCHER_AND_TECH_CHANNEL_ID',
    'account_help_forum': 'DISCORD_FORUM_ACCOUNT_HELP_CHANNEL_ID',
    'appeals_forum': 'DISCORD_FORUM_APPEALS_CHANNEL_ID',
}

ROLE_ENV_BY_ALIAS = {
    'visitor': 'VISITOR_ROLE_ID',
    'member': 'MEMBER_ROLE_ID',
    'guild_leader': 'GUILD_LEADER_ROLE_ID',
    'news': 'INTEREST_ROLE_NEWS_ID',
    'lore': 'INTEREST_ROLE_LORE_ID',
    'gameplay': 'INTEREST_ROLE_GAMEPLAY_ID',
    'events': 'INTEREST_ROLE_EVENTS_ID',
    'guilds': 'INTEREST_ROLE_GUILDS_ID',
    'media': 'INTEREST_ROLE_MEDIA_ID',
    'devlogs': 'INTEREST_ROLE_DEVLOGS_ID',
}

CHANNEL_ATTR_BY_ENV = {
    'DISCORD_ANNOUNCEMENTS_CHANNEL_ID': 'discord_announcements_channel_id',
    'DISCORD_EVENTS_CHANNEL_ID': 'discord_events_channel_id',
    'DISCORD_AUDIT_CHANNEL_ID': 'discord_audit_channel_id',
    'DISCORD_SECURITY_AUDIT_CHANNEL_ID': 'discord_security_audit_channel_id',
    'DISCORD_BUSINESS_AUDIT_CHANNEL_ID': 'discord_business_audit_channel_id',
    'DISCORD_OPS_AUDIT_CHANNEL_ID': 'discord_ops_audit_channel_id',
    'DISCORD_START_HERE_CHANNEL_ID': 'discord_start_here_channel_id',
    'DISCORD_RULES_CHANNEL_ID': 'discord_rules_channel_id',
    'DISCORD_ROLES_AND_ACCESS_CHANNEL_ID': 'discord_roles_channel_id',
    'DISCORD_FAQ_CHANNEL_ID': 'discord_faq_channel_id',
    'DISCORD_DEVLOG_CHANNEL_ID': 'discord_devlog_channel_id',
    'DISCORD_WORLD_SIGNALS_CHANNEL_ID': 'discord_world_signals_channel_id',
    'DISCORD_REPORTS_CHANNEL_ID': 'discord_reports_channel_id',
    'DISCORD_BOT_LOGS_CHANNEL_ID': 'discord_bot_logs_channel_id',
    'DISCORD_STAGE_CHANNEL_ID': 'discord_stage_channel_id',
    'DISCORD_FORUM_SUGGESTIONS_CHANNEL_ID': 'discord_forum_suggestions_channel_id',
    'DISCORD_FORUM_BUG_REPORTS_CHANNEL_ID': 'discord_forum_bug_reports_channel_id',
    'DISCORD_FORUM_GUILD_RECRUITMENT_CHANNEL_ID': 'discord_forum_guild_recruitment_channel_id',
    'DISCORD_FORUM_HELP_CHANNEL_ID': 'discord_forum_help_channel_id',
    'DISCORD_FORUM_LAUNCHER_AND_TECH_CHANNEL_ID': 'discord_forum_launcher_and_tech_channel_id',
    'DISCORD_FORUM_ACCOUNT_HELP_CHANNEL_ID': 'discord_forum_account_help_channel_id',
    'DISCORD_FORUM_APPEALS_CHANNEL_ID': 'discord_forum_appeals_channel_id',
}

ROLE_ATTR_BY_ENV = {
    'VISITOR_ROLE_ID': 'visitor_role_id',
    'MEMBER_ROLE_ID': 'member_role_id',
    'GUILD_LEADER_ROLE_ID': 'guild_leader_role_id',
    'INTEREST_ROLE_NEWS_ID': 'interest_role_news_id',
    'INTEREST_ROLE_LORE_ID': 'interest_role_lore_id',
    'INTEREST_ROLE_GAMEPLAY_ID': 'interest_role_gameplay_id',
    'INTEREST_ROLE_EVENTS_ID': 'interest_role_events_id',
    'INTEREST_ROLE_GUILDS_ID': 'interest_role_guilds_id',
    'INTEREST_ROLE_MEDIA_ID': 'interest_role_media_id',
    'INTEREST_ROLE_DEVLOGS_ID': 'interest_role_devlogs_id',
}

FORUM_ENV_KEYS = {key for key in CHANNEL_ATTR_BY_ENV if key.startswith('DISCORD_FORUM_')}
TEXT_ENV_KEYS = {
    'DISCORD_ANNOUNCEMENTS_CHANNEL_ID', 'DISCORD_EVENTS_CHANNEL_ID', 'DISCORD_AUDIT_CHANNEL_ID',
    'DISCORD_SECURITY_AUDIT_CHANNEL_ID', 'DISCORD_BUSINESS_AUDIT_CHANNEL_ID', 'DISCORD_OPS_AUDIT_CHANNEL_ID',
    'DISCORD_START_HERE_CHANNEL_ID', 'DISCORD_RULES_CHANNEL_ID', 'DISCORD_ROLES_AND_ACCESS_CHANNEL_ID',
    'DISCORD_FAQ_CHANNEL_ID', 'DISCORD_DEVLOG_CHANNEL_ID', 'DISCORD_WORLD_SIGNALS_CHANNEL_ID',
    'DISCORD_REPORTS_CHANNEL_ID', 'DISCORD_BOT_LOGS_CHANNEL_ID',
}


def _normalize_name(value: str) -> str:
    return value.strip().casefold()


def _channel_kind(channel: Any) -> str:
    if isinstance(channel, discord.ForumChannel):
        return 'forum'
    if isinstance(channel, discord.StageChannel):
        return 'stage'
    if isinstance(channel, discord.VoiceChannel):
        return 'voice'
    if isinstance(channel, discord.TextChannel):
        return 'text'
    return type(channel).__name__


def _expected_channel_kind(env_key: str) -> str:
    if env_key in FORUM_ENV_KEYS:
        return 'forum'
    if env_key == 'DISCORD_STAGE_CHANNEL_ID':
        return 'stage'
    if env_key in TEXT_ENV_KEYS:
        return 'text'
    return 'unknown'


async def _login_client(settings: Settings) -> discord.Client:
    client = discord.Client(intents=discord.Intents.none())
    await client.login(settings.discord_token)
    return client


async def run_discord_bindings_check(settings: Settings) -> int:
    errors: list[str] = []
    warnings: list[str] = []
    if not settings.discord_guild_id:
        print(json.dumps({'ok': False, 'errors': ['DISCORD_GUILD_ID is required']}, ensure_ascii=False, indent=2))
        return 4

    client = await _login_client(settings)
    try:
        guild = await client.fetch_guild(settings.discord_guild_id)
        channels = await guild.fetch_channels()
        channel_by_id = {channel.id: channel for channel in channels}
        roles = await guild.fetch_roles()
        role_by_id = {role.id: role for role in roles}

        channel_results: dict[str, Any] = {}
        for env_key, attr_name in CHANNEL_ATTR_BY_ENV.items():
            value = getattr(settings, attr_name, None)
            if not value:
                channel_results[env_key] = {'configured': False}
                continue
            channel = channel_by_id.get(int(value))
            expected = _expected_channel_kind(env_key)
            if channel is None:
                errors.append(f'{env_key} points to missing channel id {value}')
                channel_results[env_key] = {'configured': True, 'exists': False, 'id': value, 'expected_kind': expected}
                continue
            actual = _channel_kind(channel)
            if expected != 'unknown' and actual != expected:
                errors.append(f'{env_key} expects {expected}, got {actual} ({channel.name})')
            channel_results[env_key] = {'configured': True, 'exists': True, 'id': value, 'name': channel.name, 'kind': actual, 'expected_kind': expected}

        role_results: dict[str, Any] = {}
        for env_key, attr_name in ROLE_ATTR_BY_ENV.items():
            value = getattr(settings, attr_name, None)
            if not value:
                role_results[env_key] = {'configured': False}
                continue
            role = role_by_id.get(int(value))
            if role is None:
                errors.append(f'{env_key} points to missing role id {value}')
                role_results[env_key] = {'configured': True, 'exists': False, 'id': value}
                continue
            dangerous = []
            perms = role.permissions
            for perm_name in ('administrator', 'manage_guild', 'manage_roles', 'manage_channels', 'ban_members', 'kick_members'):
                if getattr(perms, perm_name, False):
                    dangerous.append(perm_name)
            if env_key.startswith('INTEREST_ROLE_') and dangerous:
                errors.append(f'{env_key} interest role has dangerous permissions: {", ".join(dangerous)}')
            role_results[env_key] = {'configured': True, 'exists': True, 'id': value, 'name': role.name, 'dangerous_permissions': dangerous}

        me = await guild.fetch_member(client.user.id) if client.user else None
        bot_permissions = {}
        if me is not None:
            permissions = me.guild_permissions
            for perm_name in ('send_messages', 'manage_messages', 'manage_threads', 'manage_roles', 'view_channel', 'moderate_members'):
                bot_permissions[perm_name] = bool(getattr(permissions, perm_name, False))
            if not bot_permissions.get('manage_roles'):
                warnings.append('Bot lacks Manage Roles at guild level; role panels may not work')
            if not bot_permissions.get('manage_threads'):
                warnings.append('Bot lacks Manage Threads at guild level; forum workflows may be limited')

        payload = {
            'ok': not errors,
            'guild': {'id': guild.id, 'name': guild.name},
            'channels': channel_results,
            'roles': role_results,
            'bot_permissions': bot_permissions,
            'warnings': warnings,
            'errors': errors,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if not errors else 4
    finally:
        await client.close()


async def run_export_discord_bindings(settings: Settings) -> int:
    if not settings.discord_guild_id:
        print('# DISCORD_GUILD_ID is required', flush=True)
        return 4
    client = await _login_client(settings)
    try:
        guild = await client.fetch_guild(settings.discord_guild_id)
        channels = await guild.fetch_channels()
        roles = await guild.fetch_roles()
        layout = load_server_layout()

        channels_by_name = {_normalize_name(channel.name): channel for channel in channels}
        roles_by_name = {_normalize_name(role.name): role for role in roles}
        lines: list[str] = []
        lines.append('# Generated by python -m nmbot.main --export-discord-bindings')
        lines.append(f'DISCORD_GUILD_ID={guild.id}')

        for category in layout.get('categories', []):
            for channel_spec in category.get('channels', []):
                alias = str(channel_spec.get('alias') or '')
                env_key = CHANNEL_ENV_BY_ALIAS.get(alias)
                if not env_key:
                    continue
                channel = channels_by_name.get(_normalize_name(str(channel_spec.get('name') or '')))
                if channel is not None:
                    lines.append(f'{env_key}={channel.id}')
                else:
                    lines.append(f'# {env_key}=  # missing channel: {channel_spec.get("name")}')

        for role_spec in layout.get('roles', []):
            alias = str(role_spec.get('alias') or '')
            env_key = ROLE_ENV_BY_ALIAS.get(alias)
            if not env_key:
                continue
            role = roles_by_name.get(_normalize_name(str(role_spec.get('name') or '')))
            if role is not None:
                lines.append(f'{env_key}={role.id}')
            else:
                lines.append(f'# {env_key}=  # missing role: {role_spec.get("name")}')

        print('\n'.join(lines))
        return 0
    finally:
        await client.close()
