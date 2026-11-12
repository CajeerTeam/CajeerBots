import { createHash } from 'node:crypto';
import { renderProviderButtons } from '@cajeerbots/message-core';
import { SerialPoller } from '@cajeerbots/core';
import { PrismaService } from '@cajeerbots/db';
import { decryptSecret } from '@cajeerbots/security';

export interface BotOutboundWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
}

export class BotOutboundWorker {
  private poller?: SerialPoller;
  private running = false;

  constructor(
    private readonly prisma = new PrismaService(),
    private readonly options: BotOutboundWorkerOptions = {},
  ) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    this.poller = new SerialPoller(this.options.pollIntervalMs ?? 5000, () => this.tick(), (error) => console.error('[worker] bot-outbound.worker tick failed:', error instanceof Error ? error.name : 'unknown'));
    await this.poller.start();
    console.log('[worker] bot outbound worker 1.0.0-rc.4 started');
  }

  async stop(): Promise<void> {
    await this.poller?.stop();
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const rows = await this.prisma.botOutboundMessage.findMany({
        where: { status: { in: ['pending', 'failed', 'sending'] }, availableAt: { lte: new Date() } },
        orderBy: { createdAt: 'asc' },
        take: this.options.batchSize ?? 25,
        include: { account: true },
      });
      for (const row of rows) await this.deliver(row.id);
    } finally {
      this.running = false;
    }
  }

  private async deliver(id: string): Promise<void> {
    const maxAttempts = this.options.maxAttempts ?? 8;
    const claim = await this.prisma.botOutboundMessage.updateMany({ where: { id, status: { in: ['pending', 'failed', 'sending'] }, availableAt: { lte: new Date() } }, data: { status: 'sending', attempts: { increment: 1 }, availableAt: new Date(Date.now() + 300_000) } });
    if (!claim.count) return;
    const row = await this.prisma.botOutboundMessage.findUniqueOrThrow({ where: { id }, include: { account: true } });
    const where = { id, status: 'sending' as const, attempts: row.attempts };
    if (row.provider === 'discord' || row.account.communityId !== row.communityId || row.account.provider !== row.provider) {
      await this.prisma.botOutboundMessage.updateMany({ where, data: { status: 'dead', lastError: 'Invalid account binding or interaction-only response' } });
      return;
    }
    try {
      if (!row.account.isActive || row.account.communityId !== row.communityId || row.account.provider !== row.provider || row.attempts > maxAttempts) throw new Error('Inactive, mismatched, exhausted or interaction-only account');
      const token = await this.resolveToken(row.account.tokenRef);
      const providerMessageId = await sendText(row.provider, token, row.targetId, row.text, row.buttons, row.id);
      await this.prisma.botOutboundMessage.updateMany({
        where,
        data: { status: 'sent', providerMessageId, sentAt: new Date(), lastError: null },
      });
    } catch (error) {
      const attempts = row.attempts;
      const dead = attempts >= maxAttempts;
      await this.prisma.botOutboundMessage.updateMany({
        where,
        data: {
          status: dead ? 'dead' : 'failed',
          lastError: error instanceof Error ? error.name : 'DeliveryError',
          availableAt: new Date(Date.now() + Math.min(300_000, 2 ** attempts * 1000)),
        },
      });
    }
  }

  private async resolveToken(tokenRef?: string | null): Promise<string> {
    if (!tokenRef) throw new Error('Bot account tokenRef пустой');
    if (tokenRef.startsWith('env:')) {
      const value = process.env[tokenRef.slice(4)];
      if (!value) throw new Error(`Env token не найден: ${tokenRef}`);
      return value;
    }
    if (tokenRef.startsWith('secret:')) {
      const key = tokenRef.slice(7);
      const secret = await this.prisma.secret.findUnique({ where: { key } });
      if (!secret) throw new Error(`Secret не найден: ${key}`);
      const encryptionKey = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
      if (!encryptionKey) throw new Error('SECRETS_ENCRYPTION_KEY или JWT_SECRET не задан');
      return decryptSecret({ algorithm: secret.algorithm as 'aes-256-gcm', ciphertext: secret.ciphertext, iv: secret.iv, authTag: secret.authTag }, encryptionKey);
    }
    return tokenRef;
  }
}

async function sendText(provider: 'telegram' | 'vk' | 'discord', token: string, targetId: string, text: string, buttons?: unknown, id?: string): Promise<string | undefined> {
  if (provider === 'telegram') return sendTelegram(token, targetId, text, buttons);
  if (provider === 'vk') return sendVk(token, targetId, text, buttons, id);
  return sendDiscord(token, targetId, text, buttons);
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


function stableRandomId(id: string): number { return createHash('sha256').update(id).digest().readUInt32BE(0) % 2147483647 + 1; }
