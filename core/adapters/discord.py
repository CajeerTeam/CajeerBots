from __future__ import annotations

import logging

from core.adapters.base import AdapterCapabilities, BotAdapter
from core.events import command_event_from_message, message_event

logger = logging.getLogger(__name__)


class DiscordAdapter(BotAdapter):
    name = "discord"
    capabilities = AdapterCapabilities(files_receive=True, roles=True, reactions=True, slash_commands=True)

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
        intents.message_content = True
        intents.messages = True
        adapter = self

        class CajeerDiscordClient(discord.Client):
            def __init__(self) -> None:
                super().__init__(intents=intents)
                self.tree = app_commands.CommandTree(self)

            async def setup_hook(self) -> None:
                guild_id = adapter.config.extra.get("guild_id")
                guild = discord.Object(id=int(guild_id)) if guild_id else None

                @self.tree.command(name="status", description="Показать состояние Cajeer Bots", guild=guild)
                async def status(interaction):  # type: ignore[no-untyped-def]
                    event = message_event(
                        source="discord",
                        platform_user_id=str(interaction.user.id),
                        platform_chat_id=str(interaction.channel_id),
                        chat_type="guild" if interaction.guild_id else "direct",
                        display_name=str(interaction.user.display_name),
                        text="/status",
                        raw={"interaction_id": str(interaction.id), "guild_id": str(interaction.guild_id or "")},
                    )
                    await adapter.publish_event(event)
                    await adapter.publish_event(command_event_from_message(event, "status", ""))
                    await interaction.response.send_message("Команда принята. Ответ будет доставлен через Cajeer Bots.", ephemeral=True)

                @self.tree.command(name="help", description="Показать команды Cajeer Bots", guild=guild)
                async def help_command(interaction):  # type: ignore[no-untyped-def]
                    event = message_event(
                        source="discord",
                        platform_user_id=str(interaction.user.id),
                        platform_chat_id=str(interaction.channel_id),
                        chat_type="guild" if interaction.guild_id else "direct",
                        display_name=str(interaction.user.display_name),
                        text="/help",
                        raw={"interaction_id": str(interaction.id), "guild_id": str(interaction.guild_id or "")},
                    )
                    await adapter.publish_event(event)
                    await adapter.publish_event(command_event_from_message(event, "help", ""))
                    await interaction.response.send_message("Команда принята. Ответ будет доставлен через Cajeer Bots.", ephemeral=True)

                if guild:
                    await self.tree.sync(guild=guild)
                else:
                    await self.tree.sync()

        client = CajeerDiscordClient()

        @client.event
        async def on_ready() -> None:
            logger.info("Discord подключён как %s", client.user)

        @client.event
        async def on_message(message) -> None:  # type: ignore[no-untyped-def]
            if message.author.bot:
                return
            event = message_event(
                source="discord",
                platform_user_id=str(message.author.id),
                platform_chat_id=str(message.channel.id),
                chat_type="guild" if getattr(message, "guild", None) else "direct",
                display_name=str(message.author.display_name),
                text=str(message.content or ""),
                raw={"message_id": str(message.id), "guild_id": str(message.guild.id) if message.guild else ""},
            )
            await self.handle_incoming_message(event)

        await client.start(self.config.token)

    async def send_message(self, target: str, text: str) -> None:
        if not self.config.token:
            return await super().send_message(target, text)
        try:
            import discord
        except ImportError as exc:
            raise RuntimeError("для Discord установите пакет discord.py") from exc
        intents = discord.Intents.default()
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready() -> None:
            channel = await client.fetch_channel(int(target))
            await channel.send(text)
            await client.close()

        await client.start(self.config.token)
        await super().send_message(target, text)
