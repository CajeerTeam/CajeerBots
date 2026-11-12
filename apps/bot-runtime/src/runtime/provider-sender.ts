import { createHash } from 'node:crypto';
import { renderProviderButtons } from '@cajeerbots/message-core';
import type { PrismaService } from '@cajeerbots/db';
import type { RuntimeBotAccount } from './load-bot-accounts.js';

export async function sendProviderText(
  prisma: PrismaService,
  account: RuntimeBotAccount,
  message: { outboundMessageId?: string; targetId?: string; text: string; buttons?: unknown },
): Promise<void> {
  if (!message.targetId) return;
  if (!account.id.startsWith('env:') && !await prisma.botAccount.findFirst({ where: { id: account.id, communityId: account.communityId, provider: account.provider, isActive: true, tokenRef: account.tokenRef } })) return;
  const id = message.outboundMessageId;
  if (!id) { await sendText(account, message.targetId, message.text, message.buttons); return; }
  const claim = await prisma.botOutboundMessage.updateMany({ where: { id, accountId: account.id, communityId: account.communityId, status: { in: ['pending', 'failed'] }, availableAt: { lte: new Date() } }, data: { status: 'sending', attempts: { increment: 1 }, availableAt: new Date(Date.now() + 300_000) } });
  if (!claim.count) return;
  const row = await prisma.botOutboundMessage.findUniqueOrThrow({ where: { id } });
  const where = { id, status: 'sending' as const, attempts: row.attempts };
  try {
    if (row.attempts > 8) throw new Error('Attempts exhausted');
    const providerMessageId = await sendText(account, row.targetId, row.text, row.buttons, id);
    await prisma.botOutboundMessage.updateMany({ where, data: { status: 'sent', providerMessageId, sentAt: new Date(), lastError: null } });
  } catch (error) {
    await prisma.botOutboundMessage.updateMany({ where, data: { status: row.attempts >= 8 ? 'dead' : 'failed', lastError: error instanceof Error ? error.name : 'DeliveryError', availableAt: new Date(Date.now() + Math.min(300_000, 2 ** row.attempts * 1000)) } });
  }
}

export async function retryOutboundMessage(prisma: PrismaService, messageId: string): Promise<{ sent: boolean; id: string }> {
  const message = await prisma.botOutboundMessage.findUnique({ where: { id: messageId }, include: { account: true } });
  if (!message) throw new Error(`Bot outbound message не найден: ${messageId}`);
  const account: RuntimeBotAccount = {
    id: message.account.id,
    communityId: message.account.communityId,
    provider: message.account.provider,
    mode: message.account.mode,
    username: message.account.username,
    tokenRef: message.account.tokenRef,
    token: await resolveAccountToken(prisma, message.account.tokenRef),
  };
  if (message.provider === 'discord' || !message.account.isActive || message.communityId !== message.account.communityId || message.provider !== message.account.provider) throw new Error('Inactive or mismatched account');
  await sendProviderText(prisma, account, { outboundMessageId: messageId, targetId: message.targetId, text: message.text, buttons: message.buttons });
  const updated = await prisma.botOutboundMessage.findUniqueOrThrow({ where: { id: messageId } });
  return { sent: updated.status === 'sent', id: messageId };
}

async function sendText(account: RuntimeBotAccount, targetId: string, text: string, buttons?: unknown, id?: string): Promise<string | undefined> {
  if (account.provider === 'telegram') return sendTelegram(account.token, targetId, text, buttons);
  if (account.provider === 'vk') return sendVk(account.token, targetId, text, buttons, id);
  return sendDiscord(account.token, targetId, text, buttons);
}

async function sendTelegram(token: string, chatId: string, text: string, buttons?: unknown): Promise<string | undefined> {
  const replyMarkup = renderProviderButtons('telegram', buttons);
  const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: 'POST', signal: AbortSignal.timeout(10_000), redirect: 'error',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text, disable_web_page_preview: true, ...(replyMarkup ? { reply_markup: replyMarkup } : {}) }),
  });
  const body = await response.json().catch(() => ({})) as { ok?: boolean; result?: { message_id?: number }; description?: string };
  if (!response.ok || body.ok !== true || !body.result?.message_id) throw new Error(`Telegram sendMessage failed: ${response.status}`);
  return body.result?.message_id ? String(body.result.message_id) : undefined;
}

async function sendVk(token: string, peerId: string, text: string, buttons?: unknown, id?: string): Promise<string | undefined> {
  const keyboard = renderProviderButtons('vk', buttons);
  const params = new URLSearchParams({ access_token: token, v: '5.199', peer_id: peerId, random_id: String(id ? stableRandomId(id) : Math.floor(Math.random() * 2147483647) + 1), message: text });
  if (keyboard) params.set('keyboard', JSON.stringify(keyboard));
  const response = await fetch('https://api.vk.com/method/messages.send', { method: 'POST', signal: AbortSignal.timeout(10_000), redirect: 'error', body: params });
  const body = await response.json().catch(() => ({})) as { response?: number; error?: { error_msg?: string } };
  if (!response.ok || body.error || typeof body.response !== 'number') throw new Error(`VK messages.send failed: ${response.status}`);
  return body.response ? String(body.response) : undefined;
}

async function sendDiscord(token: string, channelId: string, text: string, buttons?: unknown): Promise<string | undefined> {
  if (!/^\d{1,22}$/.test(channelId)) throw new Error('Invalid Discord channel ID');
  const components = renderProviderButtons('discord', buttons);
  const response = await fetch(`https://discord.com/api/v10/channels/${channelId}/messages`, {
    method: 'POST', signal: AbortSignal.timeout(10_000), redirect: 'error',
    headers: { authorization: `Bot ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ content: text, ...(components ? { components } : {}) }),
  });
  const body = await response.json().catch(() => ({})) as { id?: string; message?: string };
  if (!response.ok || !body.id) throw new Error(`Discord send message failed: ${response.status}`);
  return body.id;
}

async function resolveAccountToken(prisma: PrismaService, tokenRef?: string | null): Promise<string> {
  if (!tokenRef) throw new Error('Bot account tokenRef пустой');
  if (tokenRef.startsWith('env:')) {
    const envKey = tokenRef.slice('env:'.length);
    const value = process.env[envKey];
    if (!value) throw new Error(`Env secret не найден: ${envKey}`);
    return value;
  }
  if (tokenRef.startsWith('secret:')) {
    const { decryptSecret } = await import('@cajeerbots/security');
    const key = tokenRef.slice('secret:'.length);
    const secret = await prisma.secret.findUnique({ where: { key } });
    if (!secret) throw new Error(`Secret не найден: ${key}`);
    const encryptionKey = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
    if (!encryptionKey) throw new Error('SECRETS_ENCRYPTION_KEY или JWT_SECRET не задан');
    return decryptSecret({ algorithm: secret.algorithm as 'aes-256-gcm', ciphertext: secret.ciphertext, iv: secret.iv, authTag: secret.authTag }, encryptionKey);
  }
  return tokenRef;
}

function stableRandomId(id: string): number { return createHash('sha256').update(id).digest().readUInt32BE(0) % 2147483647 + 1; }
