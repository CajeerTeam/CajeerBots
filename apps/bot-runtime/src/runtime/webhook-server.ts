import Fastify, { type FastifyInstance } from 'fastify';
import type { PrismaService } from '@cajeerbots/db';
import type { InteractionRouter } from '@cajeerbots/interaction-core';
import type { RuntimeBotAccount } from './load-bot-accounts.js';
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
    this.server = Fastify({ logger: true });

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
      const result = await dispatchProviderWebhook(this.prisma, this.router, account, request.body as Record<string, unknown>);
      return reply.status(result.statusCode).send(result.body);
    });

    this.server.post('/runtime/webhooks/:provider/:accountId', async (request, reply) => {
      const { provider, accountId } = request.params as { provider: string; accountId: string };
      const account = accountMap.get(accountId);
      if (!account || account.provider !== provider) return reply.status(404).send({ ok: false, error: 'BOT_ACCOUNT_NOT_FOUND' });
      const result = await dispatchProviderWebhook(this.prisma, this.router, account, request.body as Record<string, unknown>);
      return reply.status(result.statusCode).send(result.body);
    });

    }
    await this.server.listen({ port, host });
  }

  async stop(): Promise<void> {
    await this.server?.close();
  }
}
