from __future__ import annotations

import asyncio
import logging
from typing import Any

from core.adapters.base import AdapterCapabilities, BotAdapter
from core.events import message_event

logger = logging.getLogger(__name__)


class TelegramAdapter(BotAdapter):
    name = "telegram"
    capabilities = AdapterCapabilities(files_receive=True, webhooks=True)

    async def on_start(self) -> None:
        if not self.config.token:
            logger.warning("токен Telegram не задан; адаптер запущен в демонстрационном режиме")
        logger.info("адаптер Telegram запущен через aiogram")
        await self.report_lifecycle("adapter.started", {"mode": self.config.extra.get("mode", "polling"), "library": "aiogram"})

    def _dispatcher(self):
        from aiogram import Dispatcher

        if self.settings.redis_url:
            try:
                from aiogram.fsm.storage.redis import RedisStorage

                return Dispatcher(storage=RedisStorage.from_url(self.settings.redis_url))
            except Exception as exc:  # pragma: no cover - fallback для неполного aiogram/redis окружения
                logger.warning("Redis FSM для aiogram недоступен, используется memory storage: %s", exc)
        return Dispatcher()

    async def run_loop(self) -> None:
        if not self.config.token:
            return await super().run_loop()
        try:
            from aiogram import Bot, F
            from aiogram.types import Message
        except ImportError as exc:
            raise RuntimeError("для Telegram установите пакет aiogram: pip install cajeer-bots[adapters]") from exc

        bot = Bot(self.config.token)
        dispatcher = self._dispatcher()
        me = await bot.get_me()
        bot_username = me.username or None

        @dispatcher.message(F.text)
        async def on_message(message: Message) -> None:
            text = message.text or ""
            event = message_event(
                source="telegram",
                platform_user_id=str(message.from_user.id if message.from_user else ""),
                platform_chat_id=str(message.chat.id),
                chat_type=str(message.chat.type),
                display_name=(message.from_user.full_name if message.from_user else None),
                text=text,
                raw={"message_id": message.message_id, "date": message.date.isoformat()},
            )
            await self.handle_incoming_message(event, bot_username=bot_username)

        try:
            if self.config.extra.get("mode") == "webhook":
                webhook_url = self.config.extra.get("webhook_url", "")
                if not webhook_url:
                    raise RuntimeError("TELEGRAM_MODE=webhook требует TELEGRAM_WEBHOOK_URL")
                await bot.set_webhook(webhook_url, secret_token=self.config.extra.get("webhook_secret") or None)
                while not self._stopping.is_set():
                    await asyncio.sleep(1)
            else:
                await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())
        finally:
            await bot.session.close()

    async def send_message(self, target: str, text: str) -> None:
        if not self.config.token:
            return await super().send_message(target, text)
        from aiogram import Bot

        bot = Bot(self.config.token)
        try:
            await bot.send_message(chat_id=target, text=text)
        finally:
            await bot.session.close()
        await super().send_message(target, text)
