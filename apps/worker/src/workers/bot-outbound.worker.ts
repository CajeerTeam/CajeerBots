import { PrismaService } from '@cajeerbots/db';
import { decryptSecret } from '@cajeerbots/security';

export interface BotOutboundWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
}

export class BotOutboundWorker {
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(
    private readonly prisma = new PrismaService(),
    private readonly options: BotOutboundWorkerOptions = {},
  ) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    await this.tick();
    this.timer = setInterval(() => void this.tick(), this.options.pollIntervalMs ?? 5000);
    console.log('[worker] bot outbound worker 1.0.0-rc.1 started');
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const rows = await this.prisma.botOutboundMessage.findMany({
        where: { status: { in: ['pending', 'failed'] }, availableAt: { lte: new Date() } },
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
    const row = await this.prisma.botOutboundMessage.update({ where: { id }, data: { status: 'sending' }, include: { account: true } });
    try {
      const token = await this.resolveToken(row.account.tokenRef);
      const providerMessageId = await sendText(row.provider, token, row.targetId, row.text);
      await this.prisma.botOutboundMessage.update({
        where: { id },
        data: { status: 'sent', attempts: { increment: 1 }, providerMessageId, sentAt: new Date(), lastError: null },
      });
    } catch (error) {
      const attempts = row.attempts + 1;
      const dead = attempts >= maxAttempts;
      await this.prisma.botOutboundMessage.update({
        where: { id },
        data: {
          status: dead ? 'dead' : 'failed',
          attempts: { increment: 1 },
          lastError: error instanceof Error ? error.message : String(error),
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

async function sendText(provider: 'telegram' | 'vk' | 'discord', token: string, targetId: string, text: string): Promise<string | undefined> {
  if (provider === 'telegram') return sendTelegram(token, targetId, text);
  if (provider === 'vk') return sendVk(token, targetId, text);
  return sendDiscord(token, targetId, text);
}

async function sendTelegram(token: string, chatId: string, text: string): Promise<string | undefined> {
  const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text, disable_web_page_preview: true }),
  });
  const body = await response.json().catch(() => ({})) as { ok?: boolean; result?: { message_id?: number }; description?: string };
  if (!response.ok || body.ok === false) throw new Error(body.description ?? `Telegram sendMessage failed: ${response.status}`);
  return body.result?.message_id ? String(body.result.message_id) : undefined;
}

async function sendVk(token: string, peerId: string, text: string): Promise<string | undefined> {
  const params = new URLSearchParams({ access_token: token, v: '5.199', peer_id: peerId, random_id: String(Date.now()), message: text });
  const response = await fetch('https://api.vk.com/method/messages.send', { method: 'POST', body: params });
  const body = await response.json().catch(() => ({})) as { response?: number; error?: { error_msg?: string } };
  if (!response.ok || body.error) throw new Error(body.error?.error_msg ?? `VK messages.send failed: ${response.status}`);
  return body.response ? String(body.response) : undefined;
}

async function sendDiscord(token: string, channelId: string, text: string): Promise<string | undefined> {
  const response = await fetch(`https://discord.com/api/v10/channels/${channelId}/messages`, {
    method: 'POST',
    headers: { authorization: `Bot ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ content: text }),
  });
  const body = await response.json().catch(() => ({})) as { id?: string; message?: string };
  if (!response.ok) throw new Error(body.message ?? `Discord send message failed: ${response.status}`);
  return body.id;
}
