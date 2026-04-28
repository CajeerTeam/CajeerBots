from __future__ import annotations

import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from nmbot.bridge import build_vk_announcement_event, build_vk_support_event, build_vk_support_reply_event
from nmbot.config import Settings
from nmbot.outbound_queue import OutboundDeliveryService
from nmbot.storage import Storage
from nmbot.vk_api import VKClient

LOGGER = logging.getLogger(__name__)

VALID_TICKET_STATUSES = {'new', 'triaged', 'in_progress', 'waiting_user', 'resolved', 'closed'}
VALID_PRIORITIES = {'low', 'normal', 'high', 'urgent'}
DEFAULT_COMMAND_ROLES: dict[str, set[str]] = {
    'say': {'admin', 'moderator'},
    'announce': {'admin'},
    'bridge': {'admin', 'moderator'},
    'tickets': {'admin', 'moderator'},
    'ticket': {'admin', 'moderator'},
    'resolve': {'admin', 'moderator'},
    'reopen': {'admin', 'moderator'},
    'status': {'admin', 'moderator'},
    'priority': {'admin', 'moderator'},
    'assign': {'admin', 'moderator'},
    'comment': {'admin', 'moderator'},
    'reply': {'admin', 'moderator'},
    'outbox': {'admin', 'moderator'},
    'retry': {'admin', 'moderator'},
    'retrydead': {'admin', 'moderator'},
    'deadletters': {'admin', 'moderator'},
}


@dataclass(slots=True)
class MessageContext:
    user_id: int
    peer_id: int
    chat_id: int | None
    text: str
    source_message_id: int | None = None
    attachments: list[str] = field(default_factory=list)

    @property
    def is_private(self) -> bool:
        return self.chat_id is None and self.peer_id < 2_000_000_000

    @property
    def is_group_chat(self) -> bool:
        return self.chat_id is not None or self.peer_id >= 2_000_000_000


class CommandHandler:
    def __init__(self, settings: Settings, vk: VKClient, storage: Storage, outbound: OutboundDeliveryService, runtime_status: dict[str, Any]) -> None:
        self.settings = settings
        self.vk = vk
        self.storage = storage
        self.outbound = outbound
        self.runtime_status = runtime_status
        self._user_rate: dict[int, deque[float]] = defaultdict(deque)
        self._peer_rate: dict[int, deque[float]] = defaultdict(deque)
        self._support_cooldowns: dict[tuple[int, int], float] = {}

    def handle(self, ctx: MessageContext) -> bool:
        text = ctx.text.strip()
        if not text.startswith(self.settings.bot_prefix):
            return False
        if not self._peer_allowed(ctx):
            LOGGER.info('Rejected command from blocked peer/user peer_id=%s user_id=%s', ctx.peer_id, ctx.user_id)
            return True
        if self.settings.ignore_private_messages and ctx.is_private:
            return True
        if self.settings.ignore_group_chats and ctx.is_group_chat:
            return True
        if not self._consume_rate_limits(ctx):
            self.vk.send_message(ctx.peer_id, 'Слишком много команд за короткое время. Подожди немного.')
            return True

        body = text[len(self.settings.bot_prefix):].strip()
        if not body:
            return False
        parts = body.split(maxsplit=1)
        command = parts[0].lower()
        argument = parts[1].strip() if len(parts) > 1 else ''

        mode = self.settings.command_mode
        if command == 'support':
            mode = self.settings.support_command_mode
        elif command == 'announce':
            mode = self.settings.announce_command_mode
        if not self._mode_allowed(ctx, mode):
            self.vk.send_message(ctx.peer_id, 'Эта команда недоступна в текущем типе диалога.')
            return True

        if command in {'help', 'start'}:
            self._send_help(ctx.peer_id)
            return True
        if command == 'ping':
            self.vk.send_message(ctx.peer_id, 'pong')
            return True
        if command == 'about':
            self.vk.send_message(ctx.peer_id, f'{self.settings.nevermine_name} — цифровая вселенная Minecraft, ориентированная на собственные механики, прогрессию, события и комьюнити.')
            return True
        if command == 'links':
            self.vk.send_message(ctx.peer_id, '\n'.join([
                f'Сайт: {self.settings.nevermine_url}',
                f'VK: {self.settings.nevermine_vk}',
                f'Telegram: {self.settings.nevermine_telegram}',
                f'Discord: {self.settings.nevermine_discord}',
            ]))
            return True
        if command == 'rules':
            self.vk.send_message(ctx.peer_id, 'Базовые правила: уважение к игрокам, без читов, без деструктивного поведения, без спама.')
            return True
        if command == 'id':
            chat_part = f', chat_id={ctx.chat_id}' if ctx.chat_id is not None else ''
            self.vk.send_message(ctx.peer_id, f'user_id={ctx.user_id}, peer_id={ctx.peer_id}{chat_part}')
            return True
        if command == 'support':
            self._send_support(ctx, argument)
            return True
        if command == 'say':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            if not argument:
                self.vk.send_message(ctx.peer_id, 'Использование: !say <текст>')
                return True
            self.vk.send_message(ctx.peer_id, argument)
            return True
        if command == 'announce':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._send_announcement(ctx, argument)
            return True
        if command in {'bridge', 'bridge_status'}:
            if not self._can_use_command(ctx, 'bridge'):
                return self._no_rights(ctx.peer_id)
            self._send_bridge_status(ctx.peer_id)
            return True
        if command == 'tickets':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._send_tickets(ctx.peer_id, argument)
            return True
        if command == 'ticket':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._send_ticket_detail(ctx.peer_id, argument)
            return True
        if command == 'resolve':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._resolve_ticket(ctx, argument)
            return True
        if command == 'reopen':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._reopen_ticket(ctx, argument)
            return True
        if command == 'status':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._set_ticket_status(ctx, argument)
            return True
        if command == 'priority':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._set_ticket_priority(ctx, argument)
            return True
        if command == 'assign':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._assign_ticket(ctx, argument)
            return True
        if command == 'comment':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._comment_ticket(ctx, argument)
            return True
        if command == 'reply':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._reply_ticket(ctx, argument)
            return True
        if command == 'outbox':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._handle_outbox_command(ctx.peer_id, argument)
            return True
        if command == 'retry':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._retry_outbox(ctx.peer_id, argument)
            return True
        if command == 'retrydead':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._retry_outbox(ctx.peer_id, argument)
            return True
        if command == 'deadletters':
            if not self._can_use_command(ctx, command):
                return self._no_rights(ctx.peer_id)
            self._send_deadletters(ctx.peer_id)
            return True

        self.vk.send_message(ctx.peer_id, f'Неизвестная команда. Используй {self.settings.bot_prefix}help')
        return True

    def _no_rights(self, peer_id: int) -> bool:
        self.vk.send_message(peer_id, 'Недостаточно прав.')
        return True

    def _role_set(self, user_id: int) -> set[str]:
        roles = {'user'}
        if user_id in self.settings.bot_moderators:
            roles.add('moderator')
        if user_id in self.settings.bot_admins:
            roles.update({'admin', 'moderator'})
        return roles

    def _command_roles(self, command: str) -> set[str]:
        configured = self.settings.command_permissions.get(command, frozenset())
        if configured:
            return set(configured)
        return set(DEFAULT_COMMAND_ROLES.get(command, {'user'}))

    def _can_use_command(self, ctx: MessageContext, command: str) -> bool:
        return bool(self._role_set(ctx.user_id).intersection(self._command_roles(command)))

    def _peer_allowed(self, ctx: MessageContext) -> bool:
        if ctx.user_id in self.settings.blocked_user_ids or ctx.peer_id in self.settings.blocked_peer_ids:
            return False
        if ctx.peer_id in self.settings.denied_peer_ids:
            return False
        if self.settings.allowed_peer_ids and ctx.peer_id not in self.settings.allowed_peer_ids:
            return False
        return True

    def _mode_allowed(self, ctx: MessageContext, mode: str) -> bool:
        if mode == 'both':
            return True
        if mode == 'private':
            return ctx.is_private
        if mode == 'groups':
            return ctx.is_group_chat
        if mode == 'none':
            return False
        return True

    def _consume_rate_limits(self, ctx: MessageContext) -> bool:
        now = time.time()
        window = float(self.settings.command_rate_limit_window_seconds)
        limit = int(self.settings.command_rate_limit_max_calls)
        for bucket, key in ((self._user_rate, ctx.user_id), (self._peer_rate, ctx.peer_id)):
            queue = bucket[key]
            while queue and queue[0] <= now - window:
                queue.popleft()
            if len(queue) >= limit:
                return False
            queue.append(now)
        return True

    def _send_support(self, ctx: MessageContext, argument: str) -> None:
        if not argument:
            self.vk.send_message(ctx.peer_id, 'Использование: !support <текст обращения>')
            return
        if len(argument) > self.settings.support_max_length:
            self.vk.send_message(ctx.peer_id, f'Обращение слишком длинное. Лимит: {self.settings.support_max_length} символов.')
            return
        cooldown_key = (ctx.user_id, ctx.peer_id)
        now = time.time()
        if now < self._support_cooldowns.get(cooldown_key, 0):
            self.vk.send_message(ctx.peer_id, 'Обращение уже отправлялось недавно. Подожди немного и попробуй снова.')
            return
        self._support_cooldowns[cooldown_key] = now + self.settings.support_cooldown_seconds
        attachments = ctx.attachments[: self.settings.attachment_max_items]
        ticket = self.storage.create_support_ticket(user_id=ctx.user_id, peer_id=ctx.peer_id, text=argument.strip(), source_message_id=ctx.source_message_id, attachments=attachments)
        event = build_vk_support_event(user_id=ctx.user_id, peer_id=ctx.peer_id, text=argument.strip(), ticket_id=ticket['ticket_id'], correlation_id=ticket['correlation_id'], attachments=attachments)
        status = self.outbound.send_or_queue(event)
        self.storage.bind_ticket_event(ticket['ticket_id'], str(event.get('event_id') or ''))
        suffix = f' Вложений: {len(attachments)}.' if attachments else ''
        message = {
            'sent': f'Обращение отправлено. Ticket: {ticket["ticket_id"]}.{suffix}',
            'queued': f'Обращение поставлено в очередь доставки. Ticket: {ticket["ticket_id"]}.{suffix}',
            'skipped': f'Ticket создан, но bridge не настроен. Ticket: {ticket["ticket_id"]}.{suffix}',
        }.get(status, f'Ticket создан. Ticket: {ticket["ticket_id"]}.{suffix}')
        self.vk.send_message(ctx.peer_id, message)

    def _parse_announce_input(self, argument: str) -> tuple[str, list[str]]:
        raw = argument.strip()
        attachments: list[str] = []
        for marker in ('|| attach=', '|| attachments=', '--attach=', '--attachments='):
            if marker in raw:
                body, tail = raw.split(marker, 1)
                raw = body.strip()
                attachments = [item.strip() for item in tail.split(',') if item.strip()]
                break
        return raw, attachments[: self.settings.attachment_max_items]

    def _send_announcement(self, ctx: MessageContext, argument: str) -> None:
        body, attachments = self._parse_announce_input(argument)
        if not body:
            self.vk.send_message(ctx.peer_id, 'Использование: !announce <текст> [|| attach=photo-1_2,doc-1_3]')
            return
        event = build_vk_announcement_event(user_id=ctx.user_id, text=body, url=self.settings.nevermine_vk, attachments=attachments)
        status = self.outbound.send_or_queue(event)
        wall_result = ''
        if self.settings.vk_wall_post_enabled:
            try:
                self.vk.wall_post(self.settings.vk_group_id, f'📣 Анонс NeverMine\n\n{body}', attachment=','.join(attachments))
                wall_result = ' + опубликован на стене VK'
            except Exception:
                LOGGER.exception('Failed to post announcement to VK wall')
                wall_result = ' + публикация на стене VK не удалась'
        if status == 'sent':
            self.vk.send_message(ctx.peer_id, f'Анонс отправлен в Discord bridge{wall_result}.')
        elif status == 'queued':
            self.vk.send_message(ctx.peer_id, f'Анонс поставлен в очередь доставки{wall_result}.')
        else:
            self.vk.send_message(ctx.peer_id, f'Анонс создан локально, но outbound bridge выключен{wall_result}.')

    def _send_bridge_status(self, peer_id: int) -> None:
        startup = self.runtime_status.get('startup_checks', {})
        self.vk.send_message(peer_id, '\n'.join([
            f'Profile: {self.settings.app_profile}',
            f'Database: {self.runtime_status.get("database_backend", self.settings.database_backend)}',
            f'Discord outbound: {"on" if self.settings.discord_bridge_url else "off"}',
            f'HTTP ingress: {"on" if self.settings.health_http_port > 0 else "off"}',
            f'Pending outbound: {self.storage.pending_outbound_count()}',
            f'Dead outbound: {self.storage.dead_outbound_count()}',
            f'Processed inbound events: {self.storage.processed_events_count()}',
            f'Remote logs: {"on" if self.settings.remote_logs_enabled else "off"}',
            f'Startup ready: {startup.get("ok")}',
        ]))

    def _send_tickets(self, peer_id: int, argument: str) -> None:
        status = argument.strip().lower() if argument.strip() else None
        rows = self.storage.list_tickets(status=status, limit=10)
        if not rows:
            self.vk.send_message(peer_id, 'Тикеты не найдены.')
            return
        self.vk.send_message(peer_id, '\n\n'.join(self._format_ticket(row) for row in rows))

    def _send_ticket_detail(self, peer_id: int, argument: str) -> None:
        ticket_id = argument.strip().upper()
        if not ticket_id:
            self.vk.send_message(peer_id, 'Использование: !ticket <ticket_id>')
            return
        ticket = self.storage.get_ticket(ticket_id)
        if not ticket:
            self.vk.send_message(peer_id, f'Тикет {ticket_id} не найден.')
            return
        lines = [
            f"{ticket['ticket_id']} | {ticket['status']} | priority={ticket.get('priority') or 'normal'}",
            f"user={ticket['user_id']} | peer={ticket['peer_id']} | assigned={ticket.get('assigned_to_user_id') or '-'}",
            f"created={ticket.get('created_at')} | last_activity={ticket.get('last_activity_at') or ticket.get('updated_at')}",
            f"text={str(ticket.get('text') or '').replace(chr(10), ' ')[:300]}",
        ]
        attachments = self._parse_attachments_json(ticket.get('attachments_json'))
        if attachments:
            lines.append(f'attachments={", ".join(attachments)}')
        lines.append('')
        for comment in self.storage.list_ticket_comments(ticket_id, limit=12):
            preview = str(comment.get('body') or '').replace('\n', ' ')[:180]
            c_att = self._parse_attachments_json(comment.get('attachments_json'))
            suffix = f' | attachments={len(c_att)}' if c_att else ''
            lines.append(f"[{comment.get('created_at')}] {comment.get('author_role')} {comment.get('direction')}: {preview}{suffix}")
        self.vk.send_message(peer_id, '\n'.join(lines)[:3500])

    def _resolve_ticket(self, ctx: MessageContext, argument: str) -> None:
        ticket_id = argument.strip().upper()
        if not ticket_id:
            self.vk.send_message(ctx.peer_id, 'Использование: !resolve <ticket_id>')
            return
        self.vk.send_message(ctx.peer_id, f'Тикет {ticket_id} переведён в resolved.' if self.storage.resolve_ticket(ticket_id, actor_user_id=ctx.user_id) else f'Тикет {ticket_id} не найден.')

    def _reopen_ticket(self, ctx: MessageContext, argument: str) -> None:
        ticket_id = argument.strip().upper()
        if not ticket_id:
            self.vk.send_message(ctx.peer_id, 'Использование: !reopen <ticket_id>')
            return
        self.vk.send_message(ctx.peer_id, f'Тикет {ticket_id} переоткрыт.' if self.storage.reopen_ticket(ticket_id, actor_user_id=ctx.user_id) else f'Тикет {ticket_id} не найден.')

    def _set_ticket_status(self, ctx: MessageContext, argument: str) -> None:
        parts = argument.split(maxsplit=1)
        if len(parts) != 2:
            self.vk.send_message(ctx.peer_id, 'Использование: !status <ticket_id> <new|triaged|in_progress|waiting_user|resolved|closed>')
            return
        ticket_id, status = parts[0].upper(), parts[1].strip().lower()
        if status not in VALID_TICKET_STATUSES:
            self.vk.send_message(ctx.peer_id, 'Недопустимый статус тикета.')
            return
        self.vk.send_message(ctx.peer_id, f'Тикет {ticket_id} -> {status}.' if self.storage.update_ticket_status(ticket_id, status, actor_user_id=ctx.user_id) else f'Тикет {ticket_id} не найден.')

    def _set_ticket_priority(self, ctx: MessageContext, argument: str) -> None:
        parts = argument.split(maxsplit=1)
        if len(parts) != 2:
            self.vk.send_message(ctx.peer_id, 'Использование: !priority <ticket_id> <low|normal|high|urgent>')
            return
        ticket_id, priority = parts[0].upper(), parts[1].strip().lower()
        if priority not in VALID_PRIORITIES:
            self.vk.send_message(ctx.peer_id, 'Недопустимый priority.')
            return
        self.vk.send_message(ctx.peer_id, f'Тикет {ticket_id} priority -> {priority}.' if self.storage.set_ticket_priority(ticket_id, priority, actor_user_id=ctx.user_id) else f'Тикет {ticket_id} не найден.')

    def _assign_ticket(self, ctx: MessageContext, argument: str) -> None:
        parts = argument.split(maxsplit=1)
        if len(parts) != 2:
            self.vk.send_message(ctx.peer_id, 'Использование: !assign <ticket_id> <vk_user_id>')
            return
        ticket_id = parts[0].upper()
        try:
            assignee = int(parts[1].strip())
        except ValueError:
            self.vk.send_message(ctx.peer_id, 'vk_user_id должен быть числом.')
            return
        self.vk.send_message(ctx.peer_id, f'Тикет {ticket_id} назначен на {assignee}.' if self.storage.assign_ticket(ticket_id, assigned_to_user_id=assignee, actor_user_id=ctx.user_id) else f'Тикет {ticket_id} не найден.')

    def _comment_ticket(self, ctx: MessageContext, argument: str) -> None:
        parts = argument.split(maxsplit=1)
        if len(parts) != 2:
            self.vk.send_message(ctx.peer_id, 'Использование: !comment <ticket_id> <текст>')
            return
        ticket_id = parts[0].upper()
        body = parts[1].strip()
        if not body:
            self.vk.send_message(ctx.peer_id, 'Комментарий пуст.')
            return
        self.vk.send_message(ctx.peer_id, f'Комментарий к {ticket_id} сохранён.' if self.storage.add_ticket_comment(ticket_id=ticket_id, body=body, author_user_id=ctx.user_id, author_role='staff', direction='internal') else f'Тикет {ticket_id} не найден.')

    def _reply_ticket(self, ctx: MessageContext, argument: str) -> None:
        parts = argument.split(maxsplit=1)
        if len(parts) != 2:
            self.vk.send_message(ctx.peer_id, 'Использование: !reply <ticket_id> <текст>')
            return
        ticket_id = parts[0].upper()
        body = parts[1].strip()
        ticket = self.storage.get_ticket(ticket_id)
        if not ticket:
            self.vk.send_message(ctx.peer_id, f'Тикет {ticket_id} не найден.')
            return
        if not body:
            self.vk.send_message(ctx.peer_id, 'Ответ пуст.')
            return
        self.storage.add_ticket_comment(ticket_id=ticket_id, body=body, author_user_id=ctx.user_id, author_role='staff', direction='outbound')
        try:
            self.vk.send_message(int(ticket['peer_id']), f'Ответ поддержки по тикету {ticket_id}:\n\n{body}')
        except Exception:
            LOGGER.exception('Failed to send direct reply for ticket %s', ticket_id)
        event = build_vk_support_reply_event(ticket_id=ticket_id, actor_user_id=ctx.user_id, text=body, peer_id=int(ticket['peer_id']), original_user_id=int(ticket['user_id']))
        self.outbound.send_or_queue(event)
        self.storage.update_ticket_status(ticket_id, 'waiting_user', actor_user_id=ctx.user_id)
        self.vk.send_message(ctx.peer_id, f'Ответ по тикету {ticket_id} отправлен.')

    def _handle_outbox_command(self, peer_id: int, argument: str) -> None:
        arg = argument.strip()
        lowered = arg.lower()
        if lowered.startswith('inspect '):
            self._inspect_outbox(peer_id, arg.split(maxsplit=1)[1])
            return
        if lowered.startswith('purge'):
            mode = arg.split(maxsplit=1)[1].strip() if len(arg.split(maxsplit=1)) == 2 else 'sent'
            removed = self.storage.purge_outbound(mode)
            self.vk.send_message(peer_id, f'Из очереди удалено записей: {removed}.')
            return
        include_dead = lowered == 'all'
        rows = self.storage.list_outbound(limit=10, include_dead=include_dead)
        if not rows:
            self.vk.send_message(peer_id, 'Очередь outbound пуста.')
            return
        lines = []
        for row in rows:
            lines.append(
                f"#{row['id']} {row['status']} attempts={row['attempts']} event={row['event_id']} http={row.get('last_http_status') or '-'} error={(row.get('last_error') or '-')[:80]}"
            )
        self.vk.send_message(peer_id, '\n'.join(lines))

    def _inspect_outbox(self, peer_id: int, reference: str) -> None:
        row = self.storage.get_outbound(reference.strip())
        if not row:
            self.vk.send_message(peer_id, f'Outbound {reference} не найден.')
            return
        self.vk.send_message(peer_id, '\n'.join([
            f"id={row['id']} event={row['event_id']}",
            f"status={row['status']} attempts={row['attempts']} http={row.get('last_http_status') or '-'}",
            f"target={row.get('target_url')}",
            f"dead_reason={row.get('dead_reason') or '-'}",
            f"dead_letter_path={row.get('dead_letter_path') or '-'}",
            f"body={(row.get('body_json') or '')[:1000]}",
        ])[:3500])

    def _retry_outbox(self, peer_id: int, argument: str) -> None:
        ref = argument.strip()
        if not ref:
            self.vk.send_message(peer_id, 'Использование: !retry <outbox_id|event_id>')
            return
        self.vk.send_message(peer_id, f'Событие {ref} повторно поставлено в очередь.' if self.storage.requeue_outbound(ref) else f'Событие {ref} не найдено.')

    def _send_deadletters(self, peer_id: int) -> None:
        rows = [row for row in self.storage.list_outbound(limit=10, include_dead=True) if row.get('status') == 'dead']
        if not rows:
            self.vk.send_message(peer_id, 'Dead-letter пуст.')
            return
        lines = [f"#{row['id']} dead event={row['event_id']} reason={(row.get('dead_reason') or row.get('last_error') or '-')[:100]} path={row.get('dead_letter_path') or '-'}" for row in rows]
        self.vk.send_message(peer_id, '\n'.join(lines))

    def _parse_attachments_json(self, raw: Any) -> list[str]:
        if not raw:
            return []
        if isinstance(raw, list):
            return [str(item) for item in raw if str(item).strip()]
        try:
            data = json.loads(str(raw))
        except Exception:
            return []
        return [str(item) for item in data if isinstance(data, list) and str(item).strip()] if isinstance(data, list) else []

    def _format_ticket(self, row: dict[str, Any]) -> str:
        body = str(row.get('text') or '').strip().replace('\n', ' ')
        if len(body) > 120:
            body = body[:117] + '...'
        assigned = row.get('assigned_to_user_id')
        return (
            f"{row['ticket_id']} | {row['status']} | priority={row.get('priority') or 'normal'} | user={row['user_id']} | peer={row['peer_id']}\n"
            f"assigned={assigned or '-'} | last_activity={row.get('last_activity_at') or row.get('updated_at')}\n"
            f"{body}"
        )

    def _send_help(self, peer_id: int) -> None:
        lines = [
            f'{self.settings.bot_prefix}help — список команд',
            f'{self.settings.bot_prefix}ping — проверка доступности',
            f'{self.settings.bot_prefix}about — о NeverMine',
            f'{self.settings.bot_prefix}links — ссылки проекта',
            f'{self.settings.bot_prefix}rules — базовые правила',
            f'{self.settings.bot_prefix}id — показать peer/user id',
            f'{self.settings.bot_prefix}support <текст> — создать тикет поддержки',
        ]
        staff_note = (
            f"\nStaff: {self.settings.bot_prefix}tickets [status], {self.settings.bot_prefix}ticket <id>, {self.settings.bot_prefix}resolve <id>, {self.settings.bot_prefix}reopen <id>, "
            f"{self.settings.bot_prefix}status <id> <status>, {self.settings.bot_prefix}priority <id> <prio>, {self.settings.bot_prefix}assign <id> <vk_user_id>, "
            f"{self.settings.bot_prefix}comment <id> <text>, {self.settings.bot_prefix}reply <id> <text>, {self.settings.bot_prefix}announce <text>, "
            f"{self.settings.bot_prefix}bridge, {self.settings.bot_prefix}outbox [all|inspect <id>|purge <sent|dead|all>], {self.settings.bot_prefix}deadletters, {self.settings.bot_prefix}retry <id>"
        )
        self.vk.send_message(peer_id, '\n'.join(lines) + staff_note)
