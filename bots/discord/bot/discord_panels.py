from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from copy import deepcopy
from typing import Any

import discord

from .config import Settings
from .locale import find_visible_english_fragments

EMBED_COLOR = 0x09ADD3
SUCCESS_COLOR = 0x2ECC71
ERROR_COLOR = 0xD9534F

DEFAULT_CONTENT: dict[str, Any] = {
    "meta": {
        "content_schema_version": 4,
        "glossary": {
            "verification": "привязка",
            "support": "поддержка",
            "appeal": "апелляция",
            "guild_recruitment": "набор в гильдию",
        },
    },
    "onboarding": {
        "title": "Начало пути — доступ к серверу NeverMine",
        "description": (
            "Добро пожаловать в NeverMine. Прежде чем открыть весь сервер, ознакомьтесь с правилами и общей навигацией. "
            "После этого нажмите кнопку ниже — бот выдаст роль участника и откроет основные категории."
        ),
    },
    "interest_roles": {
        "title": "Роли интересов",
        "description": (
            "Выбери темы, за которыми хочешь следить. Эти роли не дают власти и не меняют права модерации — "
            "они нужны для подписки на направления NeverMine и для точечных уведомлений."
        ),
    },
    "help": {
        "title": "Навигация NeverMine Discord",
        "description": "Короткая карта сервера NeverMine.",
    },
    "faq": {
        "title": "Частые вопросы и навигация",
        "description": "Основные точки входа, помощь и навигация по Discord-серверу NeverMine.",
    },
}


def _content_root(settings: Settings) -> Path:
    return settings.discord_content_file_path


def _content_candidates(settings: Settings) -> list[Path]:
    root = Path(__file__).resolve().parent.parent
    canonical = _content_root(settings)
    fallback = root / 'templates' / 'content.json'
    return [canonical, fallback] if canonical != fallback else [canonical]


def content_schema_version(settings: Settings) -> int:
    content = _load_content(settings)
    meta = content.get('meta') if isinstance(content, dict) else {}
    if isinstance(meta, dict):
        try:
            return int(meta.get('content_schema_version') or 0)
        except Exception:
            return 0
    return 0


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _channel_ref(channel_id: int | None) -> str:
    return f"<#{channel_id}>" if channel_id else "не настроено"


def _role_ref(role_id: int | None) -> str:
    return f"<@&{role_id}>" if role_id else "не настроено"



def ensure_content_layout(settings: Settings) -> Path:
    canonical = _content_root(settings)
    canonical.parent.mkdir(parents=True, exist_ok=True)
    if canonical.exists():
        return canonical
    source = next((path for path in _content_candidates(settings)[1:] if path.exists()), None)
    if source is not None:
        canonical.write_text(source.read_text(encoding='utf-8'), encoding='utf-8')
        return canonical
    canonical.write_text(json.dumps(DEFAULT_CONTENT, ensure_ascii=False, indent=2), encoding='utf-8')
    return canonical


def _load_content(settings: Settings) -> dict[str, Any]:
    ensure_content_layout(settings)
    data = deepcopy(DEFAULT_CONTENT)
    for path in _content_candidates(settings):
        try:
            if path.exists():
                raw = json.loads(path.read_text(encoding='utf-8'))
                if isinstance(raw, dict):
                    data = _deep_merge(data, raw)
                    break
        except Exception:
            continue
    return data


def get_content_pack(settings: Settings) -> dict[str, Any]:
    return _load_content(settings)


def validate_content_pack(settings: Settings) -> list[str]:
    issues: list[str] = []
    ensure_content_layout(settings)
    content = _load_content(settings)
    schema_version = content_schema_version(settings)
    if schema_version < settings.content_schema_version_required:
        issues.append(
            f"Контент-пак имеет schema_version={schema_version}, а код ожидает минимум {settings.content_schema_version_required}."
        )
    required = {
        'onboarding': ('title', 'description'),
        'interest_roles': ('title', 'description'),
        'help': ('title', 'description'),
        'faq': ('title', 'description'),
    }
    placeholder_pattern = re.compile(r"\{\{.+?\}\}|TODO|FIXME", re.IGNORECASE)
    for panel_type, keys in required.items():
        panel = content.get(panel_type)
        if not isinstance(panel, dict):
            issues.append(f"Контент панели `{panel_type}` отсутствует или повреждён.")
            continue
        for key in keys:
            value = str(panel.get(key) or '').strip()
            if not value:
                issues.append(f"Контент панели `{panel_type}` не содержит обязательное поле `{key}`.")
                continue
            if len(value) > 3500:
                issues.append(f"Поле `{panel_type}.{key}` слишком длинное для Discord UI.")
            if placeholder_pattern.search(value):
                issues.append(f"Поле `{panel_type}.{key}` содержит незаполненный placeholder или техническую пометку.")
    if settings.discord_content_require_russian:
        fragments = find_visible_english_fragments(content)
        for fragment in fragments:
            issues.append(f"В content pack найден англоязычный фрагмент: `{fragment}`")
    return issues


def get_panel_content(settings: Settings, panel_type: str) -> dict[str, Any]:
    content = _load_content(settings).get(panel_type, {})
    return content.copy() if isinstance(content, dict) else {}




def get_topic_template(settings: Settings, topic_kind: str) -> str:
    pack = _load_content(settings)
    topic_templates = pack.get('topic_templates') if isinstance(pack, dict) else {}
    if isinstance(topic_templates, dict):
        raw = topic_templates.get(topic_kind)
        if isinstance(raw, str) and raw.strip():
            return raw
    return ''


def get_ops_text(settings: Settings, key: str, default: str = '') -> str:
    pack = _load_content(settings)
    ops = pack.get('ops') if isinstance(pack, dict) else {}
    if isinstance(ops, dict):
        raw = ops.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw
    return default


def get_help_text(settings: Settings, key: str, default: str = '') -> str:
    pack = _load_content(settings)
    help_pack = pack.get('help') if isinstance(pack, dict) else {}
    if isinstance(help_pack, dict):
        raw = help_pack.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw
    return default


def get_faq_text(settings: Settings, key: str, default: str = '') -> str:
    pack = _load_content(settings)
    faq_pack = pack.get('faq') if isinstance(pack, dict) else {}
    if isinstance(faq_pack, dict):
        raw = faq_pack.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw
    return default

def get_panel_version(settings: Settings, panel_type: str) -> str:
    content = get_panel_content(settings, panel_type)
    raw = json.dumps(content, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def build_onboarding_embed(settings: Settings) -> discord.Embed:
    content = get_panel_content(settings, "onboarding")
    embed = discord.Embed(
        title=str(content.get("title") or DEFAULT_CONTENT["onboarding"]["title"]),
        description=str(content.get("description") or DEFAULT_CONTENT["onboarding"]["description"]),
        color=EMBED_COLOR,
    )
    embed.add_field(name="1. Старт", value=_channel_ref(settings.discord_start_here_channel_id), inline=False)
    embed.add_field(name="2. Правила", value=_channel_ref(settings.discord_rules_channel_id), inline=False)
    embed.add_field(name="3. Роли и доступ", value=_channel_ref(settings.discord_roles_channel_id), inline=False)
    embed.add_field(name="Что откроется", value="Мир NeverMine, игровые системы, сообщество, поддержка и голосовые каналы.", inline=False)
    return embed


def build_interest_roles_embed(settings: Settings) -> discord.Embed:
    content = get_panel_content(settings, "interest_roles")
    embed = discord.Embed(
        title=str(content.get("title") or DEFAULT_CONTENT["interest_roles"]["title"]),
        description=str(content.get("description") or DEFAULT_CONTENT["interest_roles"]["description"]),
        color=EMBED_COLOR,
    )
    embed.add_field(name="Роли", value=", ".join(
        role for role in [
            _role_ref(settings.interest_role_news_id),
            _role_ref(settings.interest_role_lore_id),
            _role_ref(settings.interest_role_gameplay_id),
            _role_ref(settings.interest_role_events_id),
            _role_ref(settings.interest_role_guilds_id),
            _role_ref(settings.interest_role_media_id),
            _role_ref(settings.interest_role_devlogs_id),
        ] if role != 'не настроено'
    ) or 'Роли интересов пока не настроены', inline=False)
    return embed




def build_panel_preview_embed(settings: Settings, panel_type: str) -> discord.Embed:
    if panel_type == "onboarding":
        return build_onboarding_embed(settings)
    if panel_type == "interest_roles":
        return build_interest_roles_embed(settings)
    return build_help_embed(settings)

def build_help_embed(settings: Settings) -> discord.Embed:
    content = get_panel_content(settings, "help")
    embed = discord.Embed(
        title=str(content.get("title") or DEFAULT_CONTENT["help"]["title"]),
        description=str(content.get("description") or DEFAULT_CONTENT["help"]["description"]),
        color=EMBED_COLOR,
    )
    embed.add_field(name="Старт", value=f"{_channel_ref(settings.discord_start_here_channel_id)} / {_channel_ref(settings.discord_rules_channel_id)} / {_channel_ref(settings.discord_roles_channel_id)}", inline=False)
    embed.add_field(name="Частые вопросы", value=_channel_ref(settings.discord_faq_channel_id), inline=False)
    embed.add_field(name="Поддержка", value=", ".join(filter(lambda x: x != 'не настроено', [
        _channel_ref(settings.discord_forum_help_channel_id),
        _channel_ref(settings.discord_forum_launcher_and_tech_channel_id),
        _channel_ref(settings.discord_forum_account_help_channel_id),
        _channel_ref(settings.discord_forum_appeals_channel_id),
    ])) or 'Не настроено', inline=False)
    embed.add_field(name="Предложения и баги", value=", ".join(filter(lambda x: x != 'не настроено', [
        _channel_ref(settings.discord_forum_suggestions_channel_id),
        _channel_ref(settings.discord_forum_bug_reports_channel_id),
    ])) or 'Не настроено', inline=False)
    embed.add_field(name="Гильдии", value=_channel_ref(settings.discord_forum_guild_recruitment_channel_id), inline=False)
    return embed


class OnboardingView(discord.ui.View):
    def __init__(self, bot: Any) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Открыть мир NeverMine", style=discord.ButtonStyle.success, custom_id="nmdiscord:onboarding:member")
    async def grant_member(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Эта кнопка работает только на сервере NeverMine.", ephemeral=True)
            return
        member_role = interaction.guild.get_role(self.bot.settings.member_role_id) if self.bot.settings.member_role_id else None
        visitor_role = interaction.guild.get_role(self.bot.settings.visitor_role_id) if self.bot.settings.visitor_role_id else None
        if member_role is None:
            await interaction.response.send_message("Роль участника не настроена у бота.", ephemeral=True)
            return
        bot_member = interaction.guild.me
        if bot_member is None or not bot_member.guild_permissions.manage_roles or bot_member.top_role.position <= member_role.position:
            await interaction.response.send_message("Бот не может выдать роль участника: проверь право управления ролями и положение роли бота выше целевой роли.", ephemeral=True)
            return
        changes: list[str] = []
        if member_role not in interaction.user.roles:
            await interaction.user.add_roles(member_role, reason="NeverMine onboarding")
            changes.append("выдана роль участника")
        if visitor_role is not None and visitor_role in interaction.user.roles and bot_member.top_role.position > visitor_role.position:
            await interaction.user.remove_roles(visitor_role, reason="NeverMine onboarding complete")
            changes.append("снята роль гостя")
        await self.bot.community_store.record_rules_acceptance(
            guild_id=str(interaction.guild_id or ''),
            discord_user_id=str(interaction.user.id),
            accepted_rules_version=self.bot.settings.rules_version,
            panel_version=get_panel_version(self.bot.settings, "onboarding"),
            metadata={"changes": changes, "source": "onboarding_button"},
        )
        await self.bot.record_audit(action="onboarding_member_granted", actor_user_id=interaction.user.id, target_user_id=interaction.user.id, status="success", payload={"changes": changes, "guild_id": interaction.guild_id, "rules_version": self.bot.settings.rules_version})
        await self.bot.queue_bridge_event("community.onboarding.completed", {"discord_user_id": str(interaction.user.id), "guild_id": str(interaction.guild_id or ''), "member_role_id": str(member_role.id), "rules_version": self.bot.settings.rules_version})
        if self.bot.settings.discord_bot_logs_channel_id:
            channel = self.bot._get_message_channel(self.bot.settings.discord_bot_logs_channel_id)
            if channel is not None:
                await channel.send(f"Пользователь {interaction.user.mention} завершил вход и принял правила версии `{self.bot.settings.rules_version}`.")
        message = "Готово. Тебе открыт доступ к основным разделам сервера."
        if not changes:
            message = "Роль участника у тебя уже есть, ничего менять не пришлось."
        await interaction.response.send_message(message, ephemeral=True)


class InterestRolesSelect(discord.ui.Select):
    def __init__(self, bot: Any) -> None:
        self.bot = bot
        options: list[discord.SelectOption] = []
        for label, role_id, emoji in [
            ("Новости", bot.settings.interest_role_news_id, "📰"),
            ("Лор", bot.settings.interest_role_lore_id, "📜"),
            ("Геймплей", bot.settings.interest_role_gameplay_id, "⚔️"),
            ("События", bot.settings.interest_role_events_id, "🎉"),
            ("Гильдии", bot.settings.interest_role_guilds_id, "🛡️"),
            ("Медиа", bot.settings.interest_role_media_id, "🎨"),
            ("Дневники разработки", bot.settings.interest_role_devlogs_id, "🛠️"),
        ]:
            if role_id:
                options.append(discord.SelectOption(label=label, value=str(role_id), emoji=emoji))
        super().__init__(
            placeholder="Выбери интересующие направления",
            min_values=0,
            max_values=len(options) if options else 1,
            options=options or [discord.SelectOption(label="Роли интересов не настроены", value="0", default=True)],
            custom_id="nmdiscord:interest-roles:select",
            disabled=not bool(options),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("Это меню работает только на сервере NeverMine.", ephemeral=True)
            return
        role_ids = {rid for rid in [
            self.bot.settings.interest_role_news_id,
            self.bot.settings.interest_role_lore_id,
            self.bot.settings.interest_role_gameplay_id,
            self.bot.settings.interest_role_events_id,
            self.bot.settings.interest_role_guilds_id,
            self.bot.settings.interest_role_media_id,
            self.bot.settings.interest_role_devlogs_id,
        ] if rid}
        if not role_ids:
            await interaction.response.send_message("Роли интересов пока не настроены.", ephemeral=True)
            return
        bot_member = interaction.guild.me
        if bot_member is None or not bot_member.guild_permissions.manage_roles:
            await interaction.response.send_message("Бот не может управлять ролями: нет права управления ролями.", ephemeral=True)
            return
        ttl = await self.bot.storage.command_cooldown(
            discord_user_id=interaction.user.id,
            command_name="interest_roles",
        )
        if ttl > 0:
            await interaction.response.send_message(f"Подожди {ttl} сек. перед повторным изменением ролей интересов.", ephemeral=True)
            return
        selected = {int(v) for v in self.values if v.isdigit()}
        current_selected = {role.id for role in interaction.user.roles if role.id in role_ids}
        add_roles = [interaction.guild.get_role(rid) for rid in selected if interaction.guild.get_role(rid) and bot_member.top_role.position > interaction.guild.get_role(rid).position]
        remove_roles = [interaction.guild.get_role(rid) for rid in role_ids - selected if interaction.guild.get_role(rid) and interaction.guild.get_role(rid) in interaction.user.roles and bot_member.top_role.position > interaction.guild.get_role(rid).position]
        if add_roles:
            await interaction.user.add_roles(*add_roles, reason="NeverMine interest roles selected")
        if remove_roles:
            await interaction.user.remove_roles(*remove_roles, reason="NeverMine interest roles updated")
        await self.bot.community_store.upsert_subscription_preferences(
            platform='discord',
            platform_user_id=str(interaction.user.id),
            preferences={'interest_roles': sorted(selected)},
        )
        added = sorted(selected - current_selected)
        removed = sorted(current_selected - selected)
        await self.bot.record_audit(action='interest_roles_updated', actor_user_id=interaction.user.id, target_user_id=interaction.user.id, status='success', payload={'selected_role_ids': sorted(selected), 'added_role_ids': added, 'removed_role_ids': removed})
        await self.bot.queue_bridge_event('community.interest_roles.updated', {'discord_user_id': str(interaction.user.id), 'selected_role_ids': sorted(selected), 'added_role_ids': added, 'removed_role_ids': removed})
        await interaction.response.send_message("Роли интересов обновлены.", ephemeral=True)


class InterestRolesView(discord.ui.View):
    def __init__(self, bot: Any) -> None:
        super().__init__(timeout=None)
        self.add_item(InterestRolesSelect(bot))


class HelpPanelView(discord.ui.View):
    def __init__(self, bot: Any) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Карта сервера", style=discord.ButtonStyle.primary, custom_id="nmdiscord:help:nav")
    async def nav(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_message(embed=build_help_embed(self.bot.settings), ephemeral=True)

    @discord.ui.button(label="Поддержка", style=discord.ButtonStyle.secondary, custom_id="nmdiscord:help:support")
    async def support(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        content = get_panel_content(self.bot.settings, "faq")
        embed = discord.Embed(title=str(content.get("title") or DEFAULT_CONTENT["faq"]["title"]), description=str(content.get("description") or DEFAULT_CONTENT["faq"]["description"]), color=SUCCESS_COLOR)
        embed.add_field(name="Общая помощь", value=_channel_ref(self.bot.settings.discord_forum_help_channel_id), inline=False)
        embed.add_field(name="Лаунчер и техника", value=_channel_ref(self.bot.settings.discord_forum_launcher_and_tech_channel_id), inline=False)
        embed.add_field(name="Аккаунт", value=_channel_ref(self.bot.settings.discord_forum_account_help_channel_id), inline=False)
        embed.add_field(name="Апелляции", value=_channel_ref(self.bot.settings.discord_forum_appeals_channel_id), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)
