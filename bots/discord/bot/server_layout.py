from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_LAYOUT_PATH = ROOT_DIR / 'templates' / 'server_layout.json'

_ROLE_COLORS = {
    'founder': '#6FE3FF',
    'cajeer_team': '#7C5CFF',
    'deeplayer_team': '#4CCBFF',
    'administrator': '#FF5C8A',
    'lead_moderator': '#FF8A4C',
    'moderator': '#FFB14C',
    'community_manager': '#3FD3B3',
    'support': '#57C7FF',
    'lore_content': '#A98BFF',
    'event_team': '#2ED6FF',
    'creator_media': '#F062FF',
    'guild_leader': '#7BE07B',
    'tester': '#9FC7FF',
    'booster': '#FF73FA',
    'member': '#C7D2E0',
    'visitor': '#8A97A8',
    'muted': '#5E6673',
    'bot': '#8899AA',
    'news': '#66D9FF',
    'lore': '#9B86FF',
    'gameplay': '#58C1FF',
    'events': '#41E0C2',
    'guilds': '#7FDB7F',
    'media': '#FF7AE6',
    'devlogs': '#8CC8FF',
}

DEFAULT_LAYOUT_SPEC: dict[str, Any] = {
    'meta': {
        'layout_schema_version': 3,
        'name': 'NeverMine Discord Layout',
        'managed_by': 'NMDiscordBot',
        'alias_binding_version': 1,
    },
    'roles': [
        {'alias': 'founder', 'name': 'Founder', 'color': _ROLE_COLORS['founder'], 'position': 220, 'managed': True},
        {'alias': 'cajeer_team', 'name': 'Cajeer Team', 'color': _ROLE_COLORS['cajeer_team'], 'position': 210, 'managed': True},
        {'alias': 'deeplayer_team', 'name': 'DeepLayer Team', 'color': _ROLE_COLORS['deeplayer_team'], 'position': 205, 'managed': True},
        {'alias': 'administrator', 'name': 'Administrator', 'color': _ROLE_COLORS['administrator'], 'position': 200, 'managed': True},
        {'alias': 'lead_moderator', 'name': 'Lead Moderator', 'color': _ROLE_COLORS['lead_moderator'], 'position': 190, 'managed': True},
        {'alias': 'moderator', 'name': 'Moderator', 'color': _ROLE_COLORS['moderator'], 'position': 180, 'managed': True},
        {'alias': 'community_manager', 'name': 'Community Manager', 'color': _ROLE_COLORS['community_manager'], 'position': 170, 'managed': True},
        {'alias': 'support', 'name': 'Support', 'color': _ROLE_COLORS['support'], 'position': 165, 'managed': True},
        {'alias': 'lore_content', 'name': 'Lore / Content', 'color': _ROLE_COLORS['lore_content'], 'position': 160, 'managed': True},
        {'alias': 'event_team', 'name': 'Event Team', 'color': _ROLE_COLORS['event_team'], 'position': 155, 'managed': True},
        {'alias': 'creator_media', 'name': 'Creator / Media', 'color': _ROLE_COLORS['creator_media'], 'position': 150, 'managed': True},
        {'alias': 'guild_leader', 'name': 'Guild Leader', 'color': _ROLE_COLORS['guild_leader'], 'position': 145, 'managed': True},
        {'alias': 'tester', 'name': 'Tester', 'color': _ROLE_COLORS['tester'], 'position': 140, 'managed': True},
        {'alias': 'booster', 'name': 'Booster', 'color': _ROLE_COLORS['booster'], 'position': 135, 'managed': True},
        {'alias': 'member', 'name': 'Member', 'color': _ROLE_COLORS['member'], 'position': 130, 'managed': True},
        {'alias': 'visitor', 'name': 'Visitor', 'color': _ROLE_COLORS['visitor'], 'position': 125, 'managed': True},
        {'alias': 'muted', 'name': 'Muted', 'color': _ROLE_COLORS['muted'], 'position': 120, 'managed': True},
        {'alias': 'bot', 'name': 'Bot', 'color': _ROLE_COLORS['bot'], 'position': 115, 'managed': True},
        {'alias': 'news', 'name': 'News', 'color': _ROLE_COLORS['news'], 'position': 110, 'managed': True},
        {'alias': 'lore', 'name': 'Lore', 'color': _ROLE_COLORS['lore'], 'position': 109, 'managed': True},
        {'alias': 'gameplay', 'name': 'Gameplay', 'color': _ROLE_COLORS['gameplay'], 'position': 108, 'managed': True},
        {'alias': 'events', 'name': 'Events', 'color': _ROLE_COLORS['events'], 'position': 107, 'managed': True},
        {'alias': 'guilds', 'name': 'Guilds', 'color': _ROLE_COLORS['guilds'], 'position': 106, 'managed': True},
        {'alias': 'media', 'name': 'Media', 'color': _ROLE_COLORS['media'], 'position': 105, 'managed': True},
        {'alias': 'devlogs', 'name': 'Devlogs', 'color': _ROLE_COLORS['devlogs'], 'position': 104, 'managed': True},
    ],
    'categories': [
        {
            'alias': 'start',
            'name': 'Начало пути',
            'public_visibility': 'visitor',
            'channels': [
                {'alias': 'start_here', 'name': 'start-here', 'type': 'text', 'read_only_public': True, 'public_visibility': 'visitor'},
                {'alias': 'rules', 'name': 'rules', 'type': 'text', 'read_only_public': True, 'public_visibility': 'visitor'},
                {'alias': 'announcements', 'name': 'announcements', 'type': 'text', 'read_only_public': True, 'public_visibility': 'visitor'},
                {'alias': 'devlog', 'name': 'devlog', 'type': 'text', 'read_only_public': True, 'public_visibility': 'visitor'},
                {'alias': 'roles_and_access', 'name': 'roles-and-access', 'type': 'text', 'read_only_public': True, 'public_visibility': 'visitor'},
                {'alias': 'faq_discord', 'name': 'faq-discord', 'type': 'text', 'read_only_public': True, 'public_visibility': 'visitor'},
            ],
        },
        {
            'alias': 'world',
            'name': 'Мир NeverMine',
            'public_visibility': 'member',
            'channels': [
                {'alias': 'world_overview', 'name': 'world-overview', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'chronicle_forum', 'name': 'forum-chronicle', 'type': 'forum', 'topic_kind': 'chronicle', 'public_visibility': 'member', 'forum_tags': ['хроника', 'официально', 'открыто', 'закрыто']},
                {'alias': 'lore_discussion_forum', 'name': 'forum-lore-discussion', 'type': 'forum', 'topic_kind': 'lore_discussion', 'public_visibility': 'member', 'forum_tags': ['лор', 'обсуждение', 'открыто', 'закрыто']},
                {'alias': 'factions', 'name': 'factions', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'hidden_systems', 'name': 'hidden-systems', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'world_signals', 'name': 'world-signals', 'type': 'text', 'read_only_public': True, 'public_visibility': 'member'},
            ],
        },
        {
            'alias': 'systems',
            'name': 'Игровые системы',
            'public_visibility': 'member',
            'channels': [
                {'alias': 'gameplay', 'name': 'gameplay', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'progression', 'name': 'progression', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'guilds_text', 'name': 'guilds', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'economy', 'name': 'economy', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'events_and_seasons', 'name': 'events-and-seasons', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'suggestions_forum', 'name': 'forum-suggestions', 'type': 'forum', 'topic_kind': 'suggestion', 'public_visibility': 'member'},
                {'alias': 'bug_reports_forum', 'name': 'forum-bug-reports', 'type': 'forum', 'topic_kind': 'bug', 'public_visibility': 'member'},
            ],
        },
        {
            'alias': 'community',
            'name': 'Сообщество',
            'public_visibility': 'member',
            'channels': [
                {'alias': 'general', 'name': 'general', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'introductions', 'name': 'introductions', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'screenshots_and_art', 'name': 'screenshots-and-art', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'clips_and_media', 'name': 'clips-and-media', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'guild_recruitment_forum', 'name': 'forum-guild-recruitment', 'type': 'forum', 'topic_kind': 'guild_recruitment', 'public_visibility': 'member'},
                {'alias': 'looking_for_party', 'name': 'looking-for-party', 'type': 'text', 'public_visibility': 'member'},
                {'alias': 'offtopic', 'name': 'offtopic', 'type': 'text', 'public_visibility': 'member'},
            ],
        },
        {
            'alias': 'voice',
            'name': 'Голос и события',
            'public_visibility': 'member',
            'channels': [
                {'alias': 'voice_general', 'name': 'General', 'type': 'voice', 'public_visibility': 'member'},
                {'alias': 'voice_party_1', 'name': 'Party 1', 'type': 'voice', 'public_visibility': 'member'},
                {'alias': 'voice_party_2', 'name': 'Party 2', 'type': 'voice', 'public_visibility': 'member'},
                {'alias': 'voice_guild_room', 'name': 'Guild Room', 'type': 'voice', 'public_visibility': 'member'},
                {'alias': 'stage_nevermine', 'name': 'Stage: NeverMine', 'type': 'stage', 'public_visibility': 'member', 'speaker_roles': ['founder', 'administrator', 'cajeer_team', 'deeplayer_team', 'community_manager', 'event_team', 'lead_moderator']},
            ],
        },
        {
            'alias': 'support',
            'name': 'Поддержка',
            'public_visibility': 'member',
            'channels': [
                {'alias': 'help_forum', 'name': 'forum-help', 'type': 'forum', 'topic_kind': 'support', 'public_visibility': 'member'},
                {'alias': 'launcher_tech_forum', 'name': 'forum-launcher-and-tech', 'type': 'forum', 'topic_kind': 'support', 'public_visibility': 'member'},
                {'alias': 'account_help_forum', 'name': 'forum-account-help', 'type': 'forum', 'topic_kind': 'support', 'public_visibility': 'member'},
                {'alias': 'appeals_forum', 'name': 'forum-appeals', 'type': 'forum', 'topic_kind': 'appeal', 'public_visibility': 'member'},
            ],
        },
        {
            'alias': 'team',
            'name': 'Команда',
            'public_visibility': 'staff',
            'staff_only': True,
            'channels': [
                {'alias': 'staff_briefing', 'name': 'staff-briefing', 'type': 'text', 'staff_only': True},
                {'alias': 'mod_chat', 'name': 'mod-chat', 'type': 'text', 'staff_only': True},
                {'alias': 'reports', 'name': 'reports', 'type': 'text', 'staff_only': True},
                {'alias': 'event_ops', 'name': 'event-ops', 'type': 'text', 'staff_only': True},
                {'alias': 'content_ops', 'name': 'content-ops', 'type': 'text', 'staff_only': True},
                {'alias': 'bot_logs', 'name': 'bot-logs', 'type': 'text', 'staff_only': True},
                {'alias': 'staff_voice', 'name': 'Staff Voice', 'type': 'voice', 'staff_only': True},
            ],
        },
    ],
}

_HEX_COLOR_RE = re.compile(r'^#[0-9a-fA-F]{6}$')
_VALID_VISIBILITY = {'visitor', 'member', 'staff'}
_VALID_CHANNEL_TYPES = {'text', 'forum', 'voice', 'stage'}


def ensure_server_layout_file(path: str | Path | None = None) -> Path:
    target = Path(path) if path else DEFAULT_LAYOUT_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_text(json.dumps(DEFAULT_LAYOUT_SPEC, ensure_ascii=False, indent=2), encoding='utf-8')
    return target


def load_server_layout(path: str | Path | None = None) -> dict[str, Any]:
    target = ensure_server_layout_file(path)
    try:
        payload = json.loads(target.read_text(encoding='utf-8'))
    except Exception:
        payload = deepcopy(DEFAULT_LAYOUT_SPEC)
    return payload if isinstance(payload, dict) else deepcopy(DEFAULT_LAYOUT_SPEC)


def iter_layout_channels(spec: dict[str, Any]) -> list[dict[str, Any]]:
    channels: list[dict[str, Any]] = []
    for category in spec.get('categories') or []:
        if not isinstance(category, dict):
            continue
        category_name = str(category.get('name') or '').strip()
        category_alias = str(category.get('alias') or '').strip()
        category_visibility = str(category.get('public_visibility') or '').strip().lower()
        category_staff_only = bool(category.get('staff_only'))
        for channel in category.get('channels') or []:
            if not isinstance(channel, dict):
                continue
            item = deepcopy(channel)
            item['category_name'] = category_name
            item['category_alias'] = category_alias
            if category_visibility and not item.get('public_visibility'):
                item['public_visibility'] = category_visibility
            if category_staff_only and 'staff_only' not in item:
                item['staff_only'] = True
            channels.append(item)
    return channels


def find_layout_channel(spec: dict[str, Any], alias: str) -> dict[str, Any] | None:
    needle = str(alias or '').strip().lower()
    for channel in iter_layout_channels(spec):
        if str(channel.get('alias') or '').strip().lower() == needle:
            return channel
    return None


def find_layout_role(spec: dict[str, Any], alias: str) -> dict[str, Any] | None:
    needle = str(alias or '').strip().lower()
    for role in spec.get('roles') or []:
        if not isinstance(role, dict):
            continue
        if str(role.get('alias') or '').strip().lower() == needle:
            return deepcopy(role)
    return None


def forum_aliases_by_kind(spec: dict[str, Any]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for channel in iter_layout_channels(spec):
        if str(channel.get('type') or '').strip().lower() != 'forum':
            continue
        kind = str(channel.get('topic_kind') or '').strip().lower()
        alias = str(channel.get('alias') or '').strip().lower()
        if kind and alias:
            result.setdefault(kind, []).append(alias)
    return result


def expected_forum_tags(spec: dict[str, Any]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for channel in iter_layout_channels(spec):
        if str(channel.get('type') or '').strip().lower() != 'forum':
            continue
        alias = str(channel.get('alias') or '').strip().lower()
        tags = []
        for item in channel.get('forum_tags') or []:
            name = str(item or '').strip()
            if name and name.lower() not in {t.lower() for t in tags}:
                tags.append(name)
        if alias:
            result[alias] = tags
    return result


def validate_server_layout(spec: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    meta = spec.get('meta') if isinstance(spec, dict) else {}
    if int((meta or {}).get('layout_schema_version') or 0) < 3:
        issues.append('server_layout.json не содержит layout_schema_version >= 3.')
    if int((meta or {}).get('alias_binding_version') or 0) < 1:
        issues.append('server_layout.json не содержит alias_binding_version >= 1.')
    if int((meta or {}).get('permission_matrix_version') or 0) < 1:
        issues.append('server_layout.json не содержит permission_matrix_version >= 1.')
    seen_role_aliases: set[str] = set()
    for role in spec.get('roles') or []:
        if not isinstance(role, dict):
            issues.append('Элемент roles должен быть объектом.')
            continue
        alias = str(role.get('alias') or '').strip().lower()
        name = str(role.get('name') or '').strip()
        if not alias:
            issues.append('Роль не содержит alias.')
            continue
        if alias in seen_role_aliases:
            issues.append(f'Дублирующийся alias роли: {alias}.')
        seen_role_aliases.add(alias)
        if not name:
            issues.append(f'Роль {alias} не содержит name.')
        color = str(role.get('color') or '').strip()
        if color and not _HEX_COLOR_RE.match(color):
            issues.append(f'Роль {alias} имеет некорректный color.')
        if role.get('position') is not None:
            try:
                int(role.get('position'))
            except Exception:
                issues.append(f'Роль {alias} имеет некорректный position.')
    seen_aliases: set[str] = set()
    channel_aliases: set[str] = set()
    for category in spec.get('categories') or []:
        if not isinstance(category, dict):
            issues.append('Элемент categories должен быть объектом.')
            continue
        if not str(category.get('name') or '').strip():
            issues.append('У категории отсутствует name.')
        visibility = str(category.get('public_visibility') or '').strip().lower()
        if visibility and visibility not in _VALID_VISIBILITY:
            issues.append(f"Категория {category.get('alias') or category.get('name') or '?'} содержит unsupported public_visibility.")
        for channel in category.get('channels') or []:
            if not isinstance(channel, dict):
                issues.append('Элемент channels должен быть объектом.')
                continue
            alias = str(channel.get('alias') or '').strip().lower()
            if not alias:
                issues.append('Канал в server layout не содержит alias.')
                continue
            if alias in seen_aliases:
                issues.append(f'Дублирующийся alias канала: {alias}.')
            seen_aliases.add(alias)
            channel_aliases.add(alias)
            ctype = str(channel.get('type') or '').strip().lower()
            if ctype not in _VALID_CHANNEL_TYPES:
                issues.append(f'Канал {alias} имеет неподдерживаемый type.')
            if not str(channel.get('name') or '').strip():
                issues.append(f'Канал {alias} не содержит name.')
            ch_visibility = str(channel.get('public_visibility') or '').strip().lower()
            if ch_visibility and ch_visibility not in _VALID_VISIBILITY:
                issues.append(f'Канал {alias} содержит unsupported public_visibility.')
            if ctype == 'forum' and not str(channel.get('topic_kind') or '').strip():
                issues.append(f'Forum-канал {alias} не содержит topic_kind.')
            if channel.get('speaker_roles') is not None and not isinstance(channel.get('speaker_roles'), list):
                issues.append(f'Канал {alias} содержит некорректный speaker_roles.')
            for role_alias in channel.get('speaker_roles') or []:
                if str(role_alias or '').strip().lower() not in seen_role_aliases:
                    issues.append(f'Канал {alias} ссылается на неизвестную роль в speaker_roles: {role_alias}.')
            for subject in (channel.get('permission_matrix') or {}).keys():
                subject_norm = str(subject or '').strip().lower()
                if subject_norm not in seen_role_aliases and subject_norm not in {'@everyone','everyone','member','visitor','staff','public'}:
                    issues.append(f'Канал {alias} содержит неизвестный subject в permission_matrix: {subject}.')
    for forum_alias in forum_aliases_by_kind(spec).values():
        for alias in forum_alias:
            if alias not in channel_aliases:
                issues.append(f'forum_aliases_by_kind ссылается на отсутствующий канал: {alias}.')
    return issues
