from __future__ import annotations

import logging
from typing import Any

from bots.discord.bot.slash import default_slash_commands
from core.adapters.base import AdapterCapabilities, BotAdapter, SendResult
from core.events import command_event_from_message, message_event

logger = logging.getLogger(__name__)


class DiscordAdapter(BotAdapter):
    name = "discord"
    capabilities = AdapterCapabilities(files_receive=True, roles=True, reactions=True, slash_commands=True)

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.client: Any | None = None

    async def on_start(self) -> None:
        if not self.config.token:
            logger.warning("токен Discord не задан; адаптер запущен в демонстрационном режиме")
        logger.info("адаптер Discord запущен через discord.py")
        await self.report_lifecycle("adapter.started", {"guild_id": self.config.extra.get("guild_id", ""), "library": "discord.py"})

    async def run_loop(self) -> None:
        if not self.config.token:
            return await super().run_loop()
        try:
            import discord
            from discord import app_commands
        except ImportError as exc:
            raise RuntimeError("для Discord установите пакет discord.py: pip install cajeer-bots[adapters]") from exc

        intents = discord.Intents.default()
        if self.config.extra.get("message_content_enabled") == "true":
            intents.message_content = True
            intents.messages = True
        adapter = self
        slash_enabled = self.config.extra.get("slash_commands_enabled", "true") == "true"

        class CajeerDiscordClient(discord.Client):
            def __init__(self) -> None:
                super().__init__(intents=intents)
                self.tree = app_commands.CommandTree(self)

            async def setup_hook(self) -> None:
                if not slash_enabled:
                    return
                guild_id = adapter.config.extra.get("guild_id")
                guild = discord.Object(id=int(guild_id)) if guild_id else None
                async def dispatch_slash(interaction, command_name: str) -> None:  # type: ignore[no-untyped-def]
                    event = message_event(source="discord", platform_user_id=str(interaction.user.id), platform_chat_id=str(interaction.channel_id), chat_type="guild" if interaction.guild_id else "direct", display_name=str(interaction.user.display_name), text=f"/{command_name}", raw={"interaction_id": str(interaction.id), "guild_id": str(interaction.guild_id or "")})
                    await adapter.publish_event(event)
                    await adapter.publish_event(command_event_from_message(event, command_name, ""))
                    await interaction.response.send_message("Команда принята. Ответ будет доставлен через Cajeer Bots.", ephemeral=True)
                for item in default_slash_commands():
                    command_name = str(item["name"]); description = str(item["description"])
                    async def callback(interaction, name=command_name):  # type: ignore[no-untyped-def]
                        await dispatch_slash(interaction, name)
                    self.tree.add_command(app_commands.Command(name=command_name, description=description, callback=callback), guild=guild)
                
                if guild:
                    await self.tree.sync(guild=guild)
                else:
                    await self.tree.sync()

        client = CajeerDiscordClient(); self.client = client
        @client.event
        async def on_ready() -> None: logger.info("Discord подключён как %s", client.user)
        @client.event
        async def on_message(message) -> None:  # type: ignore[no-untyped-def]
            if self.config.extra.get("message_content_enabled") != "true" or message.author.bot: return
            event = message_event(source="discord", platform_user_id=str(message.author.id), platform_chat_id=str(message.channel.id), chat_type="guild" if getattr(message, "guild", None) else "direct", display_name=str(message.author.display_name), text=str(message.content or ""), raw={"message_id": str(message.id), "guild_id": str(message.guild.id) if message.guild else ""})
            await self.handle_incoming_message(event)
        await client.start(self.config.token)

    async def send_message(self, target: str, text: str) -> SendResult:
        if not self.config.token:
            return await super().send_message(target, text)
        if self.client is None or getattr(self.client, "is_closed", lambda: True)():
            raise RuntimeError("Discord client не подключён; delivery для Discord должен выполняться adapter-owned loop")
        channel = self.client.get_channel(int(target)) or await self.client.fetch_channel(int(target))
        message = await channel.send(text)
        await super().send_message(target, text)
        return SendResult(ok=True, platform_message_id=str(getattr(message, "id", "") or ""), raw={"channel_id": str(target)})

    async def on_stop(self) -> None:
        if self.client is not None and not self.client.is_closed():
            await self.client.close(); self.client = None
