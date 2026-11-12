import Fastify, { type FastifyInstance } from 'fastify';
import type { PrismaService } from '@cajeerbots/db';
import type { InteractionRouter } from '@cajeerbots/interaction-core';
import type { RuntimeBotAccount } from './load-bot-accounts.js';
import { authenticateWebhook, webhookConfig } from './webhook-auth.js';
import { dispatchProviderWebhook } from './webhook-dispatcher.js';

export class BotWebhookServer {
  private server?: FastifyInstance;

  constructor(
    private readonly prisma: PrismaService,
    private readonly router: InteractionRouter,
    private readonly accounts: RuntimeBotAccount[],
    private readonly isReady: () => boolean = () => true,
    private readonly mode: string = "webhook",
  ) {}

  async start(): Promise<void> {
    const port = Number(process.env.BOT_WEBHOOK_PORT ?? 3101);
    const host = process.env.BOT_WEBHOOK_HOST ?? '0.0.0.0';
    const accountMap = new Map(this.accounts.map((account) => [account.id, account]));
    const credentials = new Set<string>();
    for (const account of this.accounts) {
      const config = webhookConfig(account);
      if (account.provider === 'discord' ? !config.publicKey || !/^[a-f0-9]{64}$/i.test(config.publicKey) : typeof config.secret !== 'string' || config.secret.length < 16 || (account.provider === 'vk' && (!config.groupId || !config.confirmation))) throw new Error(`Missing webhook credentials for ${account.id}`);
      const key = `${account.provider}:${config.publicKey ?? config.secret}`;
      if (credentials.has(key)) throw new Error('Webhook credentials must be unique per account');
      credentials.add(key);
    }
    this.server = Fastify({ logger: { redact: ['req.headers', 'req.body'] }, bodyLimit: 1024 * 1024 });
    this.server.removeContentTypeParser('application/json');
    this.server.addContentTypeParser('application/json', { parseAs: 'buffer' }, (_request, body, done) => done(null, body));

    this.server.get('/runtime/health', async (_request, reply) => {
      let ok = this.isReady();
      if (ok) { try { await this.prisma.health(); } catch { ok = false; } }
      return reply.status(ok ? 200 : 503).send({ ok, mode: this.mode });
    });
    if (this.accounts.length > 0) {
    this.server.post('/runtime/webhooks/:accountId', async (request, reply) => {
      const { accountId } = request.params as { accountId: string };
      const account = accountMap.get(accountId);
      if (!account) return reply.status(404).send({ ok: false, error: 'BOT_ACCOUNT_NOT_FOUND' });
      if (!Buffer.isBuffer(request.body)) return reply.status(415).send({ ok: false });
      let payload: Record<string, unknown>;
      try { payload = JSON.parse(request.body.toString('utf8')); if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error(); } catch { return reply.status(400).send({ ok: false }); }
      if (!authenticateWebhook(account, request.headers, request.body, payload)) return reply.status(401).send({ ok: false });
      if (!account.id.startsWith('env:') && !await this.prisma.botAccount.findFirst({ where: { id: account.id, communityId: account.communityId, provider: account.provider, mode: 'webhook', isActive: true, tokenRef: account.tokenRef } })) return reply.status(404).send({ ok: false });
      if (account.provider === 'telegram' && !Number.isSafeInteger(payload.update_id)) return reply.status(400).send({ ok: false });
      if (account.provider === 'vk' && payload.type !== 'confirmation' && (typeof payload.event_id !== 'string' || !payload.event_id)) return reply.status(400).send({ ok: false });
      if (account.provider === 'discord' && payload.type !== 1 && (typeof payload.id !== 'string' || !payload.id)) return reply.status(400).send({ ok: false });
      // Never persist provider authentication credentials.
      delete payload.secret;
      if (account.provider === 'vk' && payload.type === 'confirmation') return reply.type('text/plain').send(webhookConfig(account).confirmation);
      const result = await dispatchProviderWebhook(this.prisma, this.router, account, payload);
      return reply.status(result.statusCode).send(result.body);
    });

    this.server.post('/runtime/webhooks/:provider/:accountId', async (request, reply) => {
      const { provider, accountId } = request.params as { provider: string; accountId: string };
      const account = accountMap.get(accountId);
      if (!account || account.provider !== provider) return reply.status(404).send({ ok: false, error: 'BOT_ACCOUNT_NOT_FOUND' });
      if (!Buffer.isBuffer(request.body)) return reply.status(415).send({ ok: false });
      let payload: Record<string, unknown>;
      try { payload = JSON.parse(request.body.toString('utf8')); if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error(); } catch { return reply.status(400).send({ ok: false }); }
      if (!authenticateWebhook(account, request.headers, request.body, payload)) return reply.status(401).send({ ok: false });
      if (!account.id.startsWith('env:') && !await this.prisma.botAccount.findFirst({ where: { id: account.id, communityId: account.communityId, provider: account.provider, mode: 'webhook', isActive: true, tokenRef: account.tokenRef } })) return reply.status(404).send({ ok: false });
      if (account.provider === 'telegram' && !Number.isSafeInteger(payload.update_id)) return reply.status(400).send({ ok: false });
      if (account.provider === 'vk' && payload.type !== 'confirmation' && (typeof payload.event_id !== 'string' || !payload.event_id)) return reply.status(400).send({ ok: false });
      if (account.provider === 'discord' && payload.type !== 1 && (typeof payload.id !== 'string' || !payload.id)) return reply.status(400).send({ ok: false });
      // Never persist provider authentication credentials.
      delete payload.secret;
      if (account.provider === 'vk' && payload.type === 'confirmation') return reply.type('text/plain').send(webhookConfig(account).confirmation);
      const result = await dispatchProviderWebhook(this.prisma, this.router, account, payload);
      return reply.status(result.statusCode).send(result.body);
    });

    }
    await this.server.listen({ port, host });
  }

  async stop(): Promise<void> {
    await this.server?.close();
  }
}
