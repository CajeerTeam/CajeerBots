import { renderProviderButtons } from '@cajeerbots/message-core';
import type { PrismaService } from '@cajeerbots/db';
import type { RuntimeBotAccount } from './load-bot-accounts.js';
import { markOutboundFailed, markOutboundSent } from './runtime-records.js';

export async function sendProviderText(
  prisma: PrismaService,
  account: RuntimeBotAccount,
  message: { outboundMessageId?: string; targetId?: string; text: string; buttons?: unknown },
): Promise<void> {
  if (!message.targetId) return;
  try {
    const providerMessageId = await sendText(account, message.targetId, message.text, message.buttons);
    if (message.outboundMessageId) await markOutboundSent(prisma, message.outboundMessageId, providerMessageId);
  } catch (error) {
    if (message.outboundMessageId) await markOutboundFailed(prisma, message.outboundMessageId, error);
    throw error;
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
    token: await resolveAccountToken(prisma, message.account.tokenRef),
  };
  await prisma.botOutboundMessage.update({ where: { id: messageId }, data: { status: 'sending' } });
  await sendProviderText(prisma, account, { outboundMessageId: messageId, targetId: message.targetId, text: message.text, buttons: message.buttons });
  return { sent: true, id: messageId };
}

async function sendText(account: RuntimeBotAccount, targetId: string, text: string, buttons?: unknown): Promise<string | undefined> {
  if (account.provider === 'telegram') return sendTelegram(account.token, targetId, text, buttons);
  if (account.provider === 'vk') return sendVk(account.token, targetId, text, buttons);
  return sendDiscord(account.token, targetId, text, buttons);
}

async function sendTelegram(token: string, chatId: string, text: string, buttons?: unknown): Promise<string | undefined> {
  const replyMarkup = renderProviderButtons('telegram', buttons);
  const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text, disable_web_page_preview: true, ...(replyMarkup ? { reply_markup: replyMarkup } : {}) }),
  });
  const body = await response.json().catch(() => ({})) as { ok?: boolean; result?: { message_id?: number }; description?: string };
  if (!response.ok || body.ok === false) throw new Error(body.description ?? `Telegram sendMessage failed: ${response.status}`);
  return body.result?.message_id ? String(body.result.message_id) : undefined;
}

async function sendVk(token: string, peerId: string, text: string, buttons?: unknown): Promise<string | undefined> {
  const keyboard = renderProviderButtons('vk', buttons);
  const params = new URLSearchParams({ access_token: token, v: '5.199', peer_id: peerId, random_id: String(Date.now()), message: text });
  if (keyboard) params.set('keyboard', JSON.stringify(keyboard));
  const response = await fetch('https://api.vk.com/method/messages.send', { method: 'POST', body: params });
  const body = await response.json().catch(() => ({})) as { response?: number; error?: { error_msg?: string } };
  if (!response.ok || body.error) throw new Error(body.error?.error_msg ?? `VK messages.send failed: ${response.status}`);
  return body.response ? String(body.response) : undefined;
}

async function sendDiscord(token: string, channelId: string, text: string, buttons?: unknown): Promise<string | undefined> {
  const components = renderProviderButtons('discord', buttons);
  const response = await fetch(`https://discord.com/api/v10/channels/${channelId}/messages`, {
    method: 'POST',
    headers: { authorization: `Bot ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ content: text, ...(components ? { components } : {}) }),
  });
  const body = await response.json().catch(() => ({})) as { id?: string; message?: string };
  if (!response.ok) throw new Error(body.message ?? `Discord send message failed: ${response.status}`);
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
