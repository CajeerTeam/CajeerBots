import { VK } from 'vk-io';
import { parseCommandFromText, type InteractionRouter } from '@cajeerbots/interaction-core';
import type { PrismaService } from '@cajeerbots/db';
import type { BotAdapter } from './bot-adapter.js';
import type { RuntimeBotAccount } from '../runtime/load-bot-accounts.js';
import { dispatchRuntimeInteraction } from '../runtime/runtime-interactions.js';
import { recordInboundUpdate } from '../runtime/runtime-records.js';
import { sendProviderText } from '../runtime/provider-sender.js';

export class VkAdapter implements BotAdapter {
  private vk?: VK;

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

    this.vk = new VK({ token: account.token });
    this.vk.updates.on('message_new', async (context) => {
      const text = String(context.text ?? '').trim();
      const parsed = parseCommandFromText(text.startsWith('/') ? text : `/${normalizeVkTextCommand(text)}`);
      const inbound = await recordInboundUpdate(prisma, account, {
        providerUpdateId: context.id ? String(context.id) : `${context.peerId}:${context.createdAt ?? Date.now()}`,
        type: parsed ? 'command' : 'message',
        userExternalId: String(context.senderId),
        chatExternalId: String(context.peerId),
        payload: { id: context.id, senderId: context.senderId, peerId: context.peerId, text } as Record<string, unknown>,
      });
      if (inbound.isDuplicate) return;
      const response = await dispatchRuntimeInteraction(prisma, this.router, account, {
        type: parsed ? 'command' : 'message',
        name: parsed?.name ?? 'message',
        userId: String(context.senderId),
        payload: { text, args: parsed?.args ?? [], peerId: context.peerId },
      }, { inboundUpdateId: inbound.id, targetId: String(context.peerId) });
      if (response?.text) {
        await sendProviderText(prisma, account, { outboundMessageId: response.outboundMessageId, targetId: String(context.peerId), text: response.text, buttons: response.buttons });
      }
    });

    if (account.mode === 'polling') await this.vk.updates.start();
  }

  async stop(): Promise<void> {
    if (this.vk) await this.vk.updates.stop();
  }
}

function normalizeVkTextCommand(text: string): string {
  const normalized = text.trim().toLowerCase();
  if (normalized === 'статус') return 'status';
  if (normalized === 'помощь') return 'help';
  if (normalized === 'настройки') return 'settings';
  return normalized.replaceAll(/\s+/g, '_');
}
