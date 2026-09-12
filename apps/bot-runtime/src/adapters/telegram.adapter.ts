import { Bot } from 'grammy';
import { parseCommandFromText, type InteractionRouter } from '@cajeerbots/interaction-core';
import type { PrismaService } from '@cajeerbots/db';
import type { BotAdapter } from './bot-adapter.js';
import type { RuntimeBotAccount } from '../runtime/load-bot-accounts.js';
import { dispatchRuntimeInteraction } from '../runtime/runtime-interactions.js';
import { recordInboundUpdate } from '../runtime/runtime-records.js';
import { sendProviderText } from '../runtime/provider-sender.js';

export class TelegramAdapter implements BotAdapter {
  private bot?: Bot;

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

    this.bot = new Bot(account.token);
    this.bot.on('callback_query:data', async (ctx) => {
      const inbound = await recordInboundUpdate(prisma, account, {
        providerUpdateId: ctx.update.update_id ? String(ctx.update.update_id) : undefined,
        type: 'callback',
        userExternalId: String(ctx.from.id),
        chatExternalId: ctx.callbackQuery.message?.chat.id ? String(ctx.callbackQuery.message.chat.id) : undefined,
        payload: ctx.update as Record<string, unknown>,
      });
      if (inbound.isDuplicate) return;
      const response = await dispatchRuntimeInteraction(prisma, this.router, account, {
        type: 'callback',
        name: ctx.callbackQuery.data,
        userId: String(ctx.from.id),
        payload: { data: ctx.callbackQuery.data, messageId: ctx.callbackQuery.message?.message_id, chatId: ctx.callbackQuery.message?.chat.id },
      }, { inboundUpdateId: inbound.id, targetId: ctx.callbackQuery.message?.chat.id ? String(ctx.callbackQuery.message.chat.id) : undefined });
      await ctx.answerCallbackQuery();
      if (response?.text) {
        const targetId = ctx.callbackQuery.message?.chat.id ? String(ctx.callbackQuery.message.chat.id) : undefined;
        await sendProviderText(prisma, account, { outboundMessageId: response.outboundMessageId, targetId, text: response.text, buttons: response.buttons });
      }
    });

    this.bot.on('message:text', async (ctx) => {
      const text = ctx.message.text.trim();
      const parsed = parseCommandFromText(text);
      const inbound = await recordInboundUpdate(prisma, account, {
        providerUpdateId: ctx.update.update_id ? String(ctx.update.update_id) : undefined,
        type: parsed ? 'command' : 'message',
        userExternalId: String(ctx.from?.id ?? 'unknown'),
        chatExternalId: String(ctx.chat.id),
        payload: ctx.update as Record<string, unknown>,
      });
      if (inbound.isDuplicate) return;
      const response = await dispatchRuntimeInteraction(prisma, this.router, account, {
        type: parsed ? 'command' : 'message',
        name: parsed?.name ?? 'message',
        userId: String(ctx.from?.id ?? 'unknown'),
        payload: { text, args: parsed?.args ?? [], chatId: ctx.chat.id },
      }, { inboundUpdateId: inbound.id, targetId: String(ctx.chat.id) });
      if (response?.text) {
        await sendProviderText(prisma, account, { outboundMessageId: response.outboundMessageId, targetId: String(ctx.chat.id), text: response.text, buttons: response.buttons });
      }
    });

    if (account.mode === 'polling') await this.bot.start();
  }

  async stop(): Promise<void> {
    await this.bot?.stop();
  }
}
