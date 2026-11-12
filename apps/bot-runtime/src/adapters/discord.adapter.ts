import { Client, GatewayIntentBits } from 'discord.js';
import { renderProviderButtons } from '@cajeerbots/message-core';
import type { InteractionRouter } from '@cajeerbots/interaction-core';
import type { PrismaService } from '@cajeerbots/db';
import type { BotAdapter } from './bot-adapter.js';
import type { RuntimeBotAccount } from '../runtime/load-bot-accounts.js';
import { dispatchRuntimeInteraction } from '../runtime/runtime-interactions.js';
import { markOutboundFailed, markOutboundSent, recordInboundUpdate } from '../runtime/runtime-records.js';

export class DiscordAdapter implements BotAdapter {
  private client?: Client;

  constructor(
    private readonly router: InteractionRouter,
    private readonly options: { mode: 'polling' | 'webhook'; account: RuntimeBotAccount; prisma: PrismaService },
  ) {}

  isConfigured(): boolean {
    return Boolean(this.options.account.token);
  }

  async start(): Promise<void> {
    const { account, prisma } = this.options;
    if (!account.token) return;

    this.client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent] });
    this.client.on('interactionCreate', async (interaction) => {
      try {
      if (interaction.isChatInputCommand()) {
        const inbound = await recordInboundUpdate(prisma, account, {
          providerUpdateId: interaction.id,
          type: 'slash_command',
          userExternalId: interaction.user.id,
          chatExternalId: interaction.channelId ?? undefined,
          payload: { id: interaction.id, guildId: interaction.guildId, channelId: interaction.channelId, commandName: interaction.commandName } as Record<string, unknown>,
        });
        if (inbound.isDuplicate) return;
        const response = await dispatchRuntimeInteraction(prisma, this.router, account, {
          type: 'slash_command',
          name: interaction.commandName.replace(/^cajeer-/, ''),
          userId: interaction.user.id,
          payload: { guildId: interaction.guildId, channelId: interaction.channelId, options: interaction.options.data.map((option) => ({ name: option.name, value: option.value })) },
        }, { inboundUpdateId: inbound.id, targetId: interaction.channelId ?? undefined });
        if (!response?.text) { await interaction.reply({ content: 'Команда недоступна.', ephemeral: true, allowedMentions: { parse: [] } }); return; }
        if (response?.text) {
          try {
            await interaction.reply({ content: response.text, components: renderProviderButtons('discord', response.buttons) as never, ephemeral: response.ephemeral ?? true, allowedMentions: { parse: [] } });
            if (response.outboundMessageId) await markOutboundSent(prisma, response.outboundMessageId, interaction.id);
          } catch (error) {
            if (response.outboundMessageId) await markOutboundFailed(prisma, response.outboundMessageId, error);
            console.error('[discord] reply failed', error instanceof Error ? error.name : 'unknown');
          }
        }
      }
      if (interaction.isButton()) {
        const inbound = await recordInboundUpdate(prisma, account, {
          providerUpdateId: interaction.id,
          type: 'callback',
          userExternalId: interaction.user.id,
          chatExternalId: interaction.channelId ?? undefined,
          payload: { id: interaction.id, guildId: interaction.guildId, channelId: interaction.channelId, customId: interaction.customId } as Record<string, unknown>,
        });
        if (inbound.isDuplicate) return;
        const response = await dispatchRuntimeInteraction(prisma, this.router, account, {
          type: 'callback',
          name: interaction.customId,
          userId: interaction.user.id,
          payload: { guildId: interaction.guildId, channelId: interaction.channelId },
        }, { inboundUpdateId: inbound.id, targetId: interaction.channelId ?? undefined });
        if (!response?.text) { await interaction.reply({ content: 'Команда недоступна.', ephemeral: true, allowedMentions: { parse: [] } }); return; }
        if (response?.text) {
          try {
            await interaction.reply({ content: response.text, components: renderProviderButtons('discord', response.buttons) as never, ephemeral: response.ephemeral ?? true, allowedMentions: { parse: [] } });
            if (response.outboundMessageId) await markOutboundSent(prisma, response.outboundMessageId, interaction.id);
          } catch (error) {
            if (response.outboundMessageId) await markOutboundFailed(prisma, response.outboundMessageId, error);
            console.error('[discord] reply failed', error instanceof Error ? error.name : 'unknown');
          }
        }
      }
      } catch (error) { console.error('[discord] interaction failed', error instanceof Error ? error.name : 'unknown'); }
    });

    if (account.mode === 'polling') await this.client.login(account.token);
  }

  async stop(): Promise<void> {
    await this.client?.destroy();
  }
}
