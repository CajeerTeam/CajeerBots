from __future__ import annotations

from typing import Any

import discord
from discord import app_commands
from discord.ext import commands


def build_grouped_command_aliases(community_commands_cls: type[Any]) -> type[commands.Cog]:
    class GroupedCommandAliases(commands.Cog):
        topic = app_commands.Group(name='topic', description='Операции с темами')
        bridge = app_commands.Group(name='bridge', description='Операции с bridge')
        ops = app_commands.Group(name='ops', description='Операционный статус и диагностика')
        content = app_commands.Group(name='content', description='Операции с контентом и панелями')
        forum = app_commands.Group(name='forum', description='Управление форумами и policy')
        state = app_commands.Group(name='state', description='Экспорт и восстановление состояния')
        approval = app_commands.Group(name='approval', description='Согласования и risky operations')
        layout = app_commands.Group(name='layout', description='Layout сервера и drift')
        identity = app_commands.Group(name='identity', description='Привязки и идентичности')
        digest = app_commands.Group(name='digest', description='Тематические сводки и алерты')

        def __init__(self, bot: "NMDiscordBot") -> None:
            self.bot = bot

        def _community(self) -> CommunityCommands:
            cog = self.bot.get_cog('CommunityCommands')
            assert isinstance(cog, community_commands_cls)
            return cog

        @topic.command(name='status', description='Показать статус темы')
        @app_commands.default_permissions(manage_threads=True)
        async def topic_status_group(self, interaction: discord.Interaction, thread_id: str | None = None) -> None:
            await community_commands_cls.topic_status.callback(self._community(), interaction, thread_id)

        @topic.command(name='claim', description='Назначить себя ответственным за тему')
        @app_commands.default_permissions(manage_threads=True)
        async def topic_claim_group(self, interaction: discord.Interaction, thread_id: str | None = None) -> None:
            await community_commands_cls.topic_claim.callback(self._community(), interaction, thread_id)

        @topic.command(name='triage', description='Изменить статус темы')
        @app_commands.default_permissions(manage_threads=True)
        async def topic_triage_group(self, interaction: discord.Interaction, thread_id: str | None = None, status: str = 'открыто', note: str | None = None) -> None:
            await community_commands_cls.topic_triage.callback(self._community(), interaction, thread_id, status, note)

        @topic.command(name='export', description='Экспортировать тему')
        @app_commands.default_permissions(manage_threads=True)
        async def topic_export_group(self, interaction: discord.Interaction, thread_id: str | None = None, mode: str = 'auto') -> None:
            await community_commands_cls.topic_export.callback(self._community(), interaction, thread_id, mode)

        @topic.command(name='update', description='Обновить заголовок или текст темы')
        @app_commands.default_permissions(manage_threads=True)
        async def topic_update_group(self, interaction: discord.Interaction, title: str | None = None, body: str | None = None, thread_id: str | None = None) -> None:
            await community_commands_cls.topic_update.callback(self._community(), interaction, title, body, thread_id)

        @bridge.command(name='status', description='Показать статус bridge')
        @app_commands.default_permissions(manage_guild=True)
        async def bridge_status_group(self, interaction: discord.Interaction) -> None:
            await community_commands_cls.bridge_status.callback(self._community(), interaction)

        @bridge.command(name='history', description='Показать историю bridge')
        @app_commands.default_permissions(manage_guild=True)
        async def bridge_history_group(self, interaction: discord.Interaction, destination: str | None = None, event_kind: str | None = None, hours: app_commands.Range[int,1,168] = 24) -> None:
            await community_commands_cls.history_snapshot.callback(self._community(), interaction, destination, event_kind, hours)

        @ops.command(name='status', description='Показать ops-статус')
        @app_commands.default_permissions(manage_guild=True)
        async def ops_status_group(self, interaction: discord.Interaction, mode: str = 'общий') -> None:
            await community_commands_cls.ops_status.callback(self._community(), interaction, mode)

        @ops.command(name='audit', description='Поиск по аудиту')
        @app_commands.default_permissions(manage_guild=True)
        async def ops_audit_group(self, interaction: discord.Interaction, actor_user_id: str | None = None, target_user_id: str | None = None, status: str | None = None, category: str | None = None) -> None:
            await community_commands_cls.audit_search.callback(self._community(), interaction, actor_user_id, target_user_id, status, category, 20)

        @ops.command(name='surface', description='Показать каноническую схему slash-команд')
        @app_commands.default_permissions(manage_guild=True)
        async def ops_surface_group(self, interaction: discord.Interaction) -> None:
            await community_commands_cls.command_surface_report.callback(self._community(), interaction)

        @content.command(name='reload', description='Перезагрузить content pack')
        @app_commands.default_permissions(manage_messages=True)
        async def content_reload_group(self, interaction: discord.Interaction, mode: str = 'apply-and-reconcile') -> None:
            await community_commands_cls.content_reload.callback(self._community(), interaction, mode)

        @content.command(name='announcement-update', description='Обновить существующее объявление')
        @app_commands.default_permissions(manage_messages=True)
        async def content_announcement_update_group(self, interaction: discord.Interaction, message_id: str, text: str, title: str | None = None) -> None:
            await community_commands_cls.announcement_update.callback(self._community(), interaction, message_id, text, title)

        @content.command(name='announcement-delete', description='Удалить объявление')
        @app_commands.default_permissions(manage_messages=True)
        async def content_announcement_delete_group(self, interaction: discord.Interaction, message_id: str) -> None:
            await community_commands_cls.announcement_delete.callback(self._community(), interaction, message_id)

        @content.command(name='devlog-publish', description='Опубликовать запись в devlog')
        @app_commands.default_permissions(manage_messages=True)
        async def content_devlog_publish_group(self, interaction: discord.Interaction, title: str, text: str, url: str | None = None) -> None:
            await community_commands_cls.devlog_publish.callback(self._community(), interaction, title, text, url)

        @content.command(name='devlog-update', description='Обновить запись devlog')
        @app_commands.default_permissions(manage_messages=True)
        async def content_devlog_update_group(self, interaction: discord.Interaction, message_id: str, title: str, text: str, url: str | None = None) -> None:
            await community_commands_cls.devlog_update.callback(self._community(), interaction, message_id, title, text, url)

        @content.command(name='devlog-delete', description='Удалить запись devlog')
        @app_commands.default_permissions(manage_messages=True)
        async def content_devlog_delete_group(self, interaction: discord.Interaction, message_id: str) -> None:
            await community_commands_cls.devlog_delete.callback(self._community(), interaction, message_id)

        @forum.command(name='policy', description='Показать политику forum-kind')
        @app_commands.default_permissions(manage_threads=True)
        async def forum_policy_view_group(self, interaction: discord.Interaction, topic_kind: str) -> None:
            await community_commands_cls.forum_policy_view.callback(self._community(), interaction, topic_kind)

        @state.command(name='export', description='Выгрузить operational state')
        @app_commands.default_permissions(manage_guild=True)
        async def state_export_group(self, interaction: discord.Interaction) -> None:
            await community_commands_cls.state_export.callback(self._community(), interaction)

        @approval.command(name='recent', description='Показать очередь согласований')
        @app_commands.default_permissions(manage_guild=True)
        async def approval_recent_group(self, interaction: discord.Interaction, limit: int = 10, status: str | None = None) -> None:
            await community_commands_cls.approval_recent.callback(self._community(), interaction, limit, status)

        @approval.command(name='decide', description='Одобрить или отклонить согласование')
        @app_commands.default_permissions(manage_guild=True)
        async def approval_decide_group(self, interaction: discord.Interaction, request_id: int, decision: str, note: str | None = None) -> None:
            await community_commands_cls.approval_decide.callback(self._community(), interaction, request_id, decision, note)

        @layout.command(name='repair', description='Проверить и починить layout')
        @app_commands.default_permissions(manage_guild=True)
        async def layout_repair_group(self, interaction: discord.Interaction, scope: str = 'all', apply_changes: bool = False) -> None:
            await community_commands_cls.layout_repair.callback(self._community(), interaction, apply=apply_changes, confirm=apply_changes, scope=scope)

        @layout.command(name='legacy-review', description='Показать legacy-ресурсы layout')
        @app_commands.default_permissions(manage_guild=True)
        async def layout_legacy_review_group(self, interaction: discord.Interaction, due_only: bool = False, limit: int = 10) -> None:
            await community_commands_cls.layout_legacy_review.callback(self._community(), interaction, due_only, limit)

        @layout.command(name='legacy-cleanup', description='Удалить просроченные legacy-ресурсы')
        @app_commands.default_permissions(manage_guild=True)
        async def layout_legacy_cleanup_group(self, interaction: discord.Interaction, limit: int = 10, apply: bool = False) -> None:
            await community_commands_cls.layout_legacy_cleanup.callback(self._community(), interaction, limit, apply)

        @identity.command(name='link-status', description='Проверить статус привязки')
        async def identity_link_status_group(self, interaction: discord.Interaction) -> None:
            await community_commands_cls.verify_status.callback(self._community(), interaction)

        @digest.command(name='targeted', description='Отправить тематическую сводку')
        @app_commands.default_permissions(manage_guild=True)
        async def digest_targeted_group(self, interaction: discord.Interaction, digest_kind: str = 'staff', channel_id: str | None = None) -> None:
            await community_commands_cls.targeted_digest_now.callback(self._community(), interaction, digest_kind, channel_id)

        @digest.command(name='schedule', description='Запланировать тематическую сводку')
        @app_commands.default_permissions(manage_guild=True)
        async def digest_schedule_group(self, interaction: discord.Interaction, digest_kind: str = 'staff', hours_from_now: int = 24, channel_id: str | None = None, repeat_every_hours: int = 0, repeat_count: int = 0) -> None:
            await community_commands_cls.targeted_digest_schedule.callback(self._community(), interaction, digest_kind, hours_from_now, channel_id, repeat_every_hours, repeat_count)

        @digest.command(name='calendar', description='Запланировать тематическую сводку по календарю')
        @app_commands.default_permissions(manage_guild=True)
        async def digest_calendar_group(self, interaction: discord.Interaction, digest_kind: str = 'staff', local_time: str = '09:00', weekday: str | None = None, timezone_name: str = 'Europe/Berlin', channel_id: str | None = None, repeat_count: int = 0, weekday_set: str | None = None, day_of_month: int | None = None) -> None:
            await community_commands_cls.targeted_digest_calendar.callback(self._community(), interaction, digest_kind, local_time, weekday, timezone_name, channel_id, repeat_count, weekday_set, day_of_month)

        @digest.command(name='rules-status', description='Показать статус повторного принятия правил')
        @app_commands.default_permissions(manage_guild=True)
        async def digest_rules_status_group(self, interaction: discord.Interaction, limit: int = 10) -> None:
            await community_commands_cls.rules_reacceptance_status.callback(self._community(), interaction, limit)

        @ops.command(name='capabilities', description='Показать capability self-report')
        @app_commands.default_permissions(manage_guild=True)
        async def ops_capabilities_group(self, interaction: discord.Interaction) -> None:
            await community_commands_cls.capability_report.callback(self._community(), interaction)

        @bridge.command(name='preview', description='Показать маршрутизацию bridge-события')
        @app_commands.default_permissions(manage_guild=True)
        async def bridge_preview_group(self, interaction: discord.Interaction, event_kind: str = 'community.announcement.created') -> None:
            await community_commands_cls.bridge_preview.callback(self._community(), interaction, event_kind)

        @bridge.command(name='coverage', description='Показать покрытие event-contract bridge-routes')
        @app_commands.default_permissions(manage_guild=True)
        async def bridge_coverage_group(self, interaction: discord.Interaction) -> None:
            await community_commands_cls.event_coverage.callback(self._community(), interaction)

    return GroupedCommandAliases
