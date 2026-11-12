import type { PrismaService } from '@cajeerbots/db';
import type { InteractionRouter } from '@cajeerbots/interaction-core';
import { renderProviderButtons } from '@cajeerbots/message-core';
import { parseCommandFromText } from '@cajeerbots/interaction-core';
import type { RuntimeBotAccount } from './load-bot-accounts.js';
import { dispatchRuntimeInteraction } from './runtime-interactions.js';
import { recordInboundUpdate } from './runtime-records.js';
import { sendProviderText } from './provider-sender.js';

export interface WebhookDispatchResult {
  statusCode: number;
  body: unknown;
}

export async function dispatchProviderWebhook(
  prisma: PrismaService,
  router: InteractionRouter,
  account: RuntimeBotAccount,
  payload: Record<string, unknown>,
): Promise<WebhookDispatchResult> {
  if (account.provider === 'telegram') return dispatchTelegram(prisma, router, account, payload);
  if (account.provider === 'vk') return dispatchVk(prisma, router, account, payload);
  return dispatchDiscord(prisma, router, account, payload);
}

async function dispatchTelegram(prisma: PrismaService, router: InteractionRouter, account: RuntimeBotAccount, payload: Record<string, unknown>): Promise<WebhookDispatchResult> {
  const message = asRecord(payload.message);
  const callback = asRecord(payload.callback_query);
  const chat = asRecord(message?.chat ?? asRecord(callback?.message)?.chat);
  const from = asRecord(message?.from ?? callback?.from);
  const text = String(message?.text ?? '').trim();
  const callbackData = callback?.data ? String(callback.data) : undefined;
  const parsed = callbackData ? null : parseCommandFromText(text);
  const type = callbackData ? 'callback' : parsed ? 'command' : 'message';
  const targetId = chat?.id === undefined ? undefined : String(chat.id);
  const userId = from?.id === undefined ? 'unknown' : String(from.id);
  const providerUpdateId = payload.update_id === undefined ? undefined : String(payload.update_id);
  const inbound = await recordInboundUpdate(prisma, account, { providerUpdateId, type, userExternalId: userId, chatExternalId: targetId, payload });
  if (inbound.isDuplicate) return { statusCode: ['handled', 'ignored'].includes(inbound.status ?? '') ? 200 : 503, body: { ok: true, duplicate: true } };

  const response = await dispatchRuntimeInteraction(prisma, router, account, {
    type,
    name: callbackData ?? parsed?.name ?? 'message',
    userId,
    payload: { text, data: callbackData, args: parsed?.args ?? [], chatId: targetId, update: providerUpdateId },
  }, { inboundUpdateId: inbound.id, targetId });

  if (inbound.id && (await prisma.botInboundUpdate.findUnique({ where: { id: inbound.id } }))?.status === 'failed') return { statusCode: 503, body: { ok: false } };
  if (response?.text && targetId) await sendProviderText(prisma, account, { outboundMessageId: response.outboundMessageId, targetId, text: response.text, buttons: response.buttons });
  return { statusCode: 200, body: { ok: true } };
}

async function dispatchVk(prisma: PrismaService, router: InteractionRouter, account: RuntimeBotAccount, payload: Record<string, unknown>): Promise<WebhookDispatchResult> {
  if (payload.type !== 'message_new') return { statusCode: 200, body: 'ok' };
  const object = asRecord(payload.object) ?? {};
  const message = asRecord(object.message) ?? object;
  const text = String(message.text ?? '').trim();
  const parsed = parseCommandFromText(text.startsWith('/') ? text : `/${normalizeVkTextCommand(text)}`);
  const targetId = message.peer_id === undefined ? undefined : String(message.peer_id);
  const userId = message.from_id === undefined ? 'unknown' : String(message.from_id);
  const type = parsed ? 'command' : 'message';
  const providerUpdateId = payload.event_id ? String(payload.event_id) : message.id ? String(message.id) : undefined;
  const inbound = await recordInboundUpdate(prisma, account, { providerUpdateId, type, userExternalId: userId, chatExternalId: targetId, payload });
  if (inbound.isDuplicate) return { statusCode: ['handled', 'ignored'].includes(inbound.status ?? '') ? 200 : 503, body: 'ok' };

  const response = await dispatchRuntimeInteraction(prisma, router, account, {
    type,
    name: parsed?.name ?? 'message',
    userId,
    payload: { text, args: parsed?.args ?? [], peerId: targetId },
  }, { inboundUpdateId: inbound.id, targetId });
  if (inbound.id && (await prisma.botInboundUpdate.findUnique({ where: { id: inbound.id } }))?.status === 'failed') return { statusCode: 503, body: { ok: false } };
  if (response?.text && targetId) await sendProviderText(prisma, account, { outboundMessageId: response.outboundMessageId, targetId, text: response.text, buttons: response.buttons });
  return { statusCode: 200, body: 'ok' };
}

async function dispatchDiscord(prisma: PrismaService, router: InteractionRouter, account: RuntimeBotAccount, payload: Record<string, unknown>): Promise<WebhookDispatchResult> {
  if (payload.type === 1) return { statusCode: 200, body: { type: 1 } };
  const data = asRecord(payload.data) ?? {};
  delete payload.token;
  const member = asRecord(payload.member);
  const user = asRecord(payload.user ?? member?.user);
  const channelId = payload.channel_id === undefined ? undefined : String(payload.channel_id);
  const commandName = payload.type === 3 ? String(data.custom_id ?? 'interaction') : data.name ? String(data.name).replace(/^cajeer-/, '') : 'interaction';
  const userId = user?.id === undefined ? 'unknown' : String(user.id);
  const providerUpdateId = payload.id === undefined ? undefined : String(payload.id);
  const type = payload.type === 3 ? 'callback' : 'slash_command';
  const inbound = await recordInboundUpdate(prisma, account, { providerUpdateId, type, userExternalId: userId, chatExternalId: channelId, payload });
  if (inbound.isDuplicate) return { statusCode: 200, body: { type: 4, data: { content: 'Запрос уже обработан.', flags: 64 } } };

  const response = await dispatchRuntimeInteraction(prisma, router, account, {
    type,
    name: commandName,
    userId,
    payload: { guildId: payload.guild_id, channelId, options: data.options ?? [] },
  }, { inboundUpdateId: inbound.id, targetId: channelId });

  if (!response?.text) return { statusCode: 200, body: { type: 4, data: { content: 'Команда недоступна.', flags: 64 } } };
  if (response.outboundMessageId) await prisma.botOutboundMessage.update({ where: { id: response.outboundMessageId }, data: { status: 'sent', attempts: { increment: 1 }, sentAt: new Date(), providerMessageId: providerUpdateId } });
  return { statusCode: 200, body: { type: 4, data: { content: response.text, allowed_mentions: { parse: [] }, components: renderProviderButtons('discord', response.buttons), flags: response.ephemeral === false ? 0 : 64 } } };
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null ? value as Record<string, unknown> : undefined;
}

function normalizeVkTextCommand(text: string): string {
  const normalized = text.trim().toLowerCase();
  if (normalized === 'статус') return 'status';
  if (normalized === 'помощь') return 'help';
  if (normalized === 'настройки') return 'settings';
  return normalized.replaceAll(/\s+/g, '_');
}
