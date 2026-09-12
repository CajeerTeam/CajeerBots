import { PrismaService } from '@cajeerbots/db';
import { signPayload } from '@cajeerbots/security';

export interface OutboxWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
}

export class OutboxWorker {
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(
    private readonly prisma = new PrismaService(),
    private readonly options: OutboxWorkerOptions = {},
  ) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    await this.tick();
    this.timer = setInterval(() => void this.tick(), this.options.pollIntervalMs ?? 5000);
    console.log('[worker] outbox worker 1.0.0-rc.1 started');
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const messages = await this.prisma.outboxMessage.findMany({
        where: { status: { in: ['pending', 'failed'] }, availableAt: { lte: new Date() } },
        orderBy: { createdAt: 'asc' },
        take: this.options.batchSize ?? 25,
      });
      for (const message of messages) await this.deliver(message.id);
    } finally {
      this.running = false;
    }
  }

  private async deliver(id: string): Promise<void> {
    const maxAttempts = this.options.maxAttempts ?? 8;
    const message = await this.prisma.outboxMessage.update({
      where: { id },
      data: { status: 'processing', attempts: { increment: 1 } },
    });

    const endpoints = await this.prisma.webhookEndpoint.findMany({
      where: { isActive: true, OR: [{ events: { has: message.topic } }, { events: { has: '*' } }] },
    });

    if (endpoints.length === 0) {
      await this.prisma.outboxMessage.update({
        where: { id },
        data: { status: 'delivered', deliveredAt: new Date(), headers: { ...(asRecord(message.headers)), noEndpoints: true } as never },
      });
      return;
    }

    const body = JSON.stringify(message.payload);
    const results = [] as Array<{ ok: boolean; error?: string }>;

    for (const endpoint of endpoints) {
      const delivery = await this.prisma.webhookDelivery.upsert({
        where: { outboxMessageId_endpointId: { outboxMessageId: id, endpointId: endpoint.id } },
        create: { outboxMessageId: id, endpointId: endpoint.id, status: 'processing', attempts: 1 },
        update: { status: 'processing', attempts: { increment: 1 }, lastError: null, responseStatus: null, responseBody: null },
      });

      try {
        const headers: Record<string, string> = {
          'content-type': 'application/json',
          'x-cajeer-event': message.topic,
          'x-cajeer-outbox-id': id,
          'x-cajeer-delivery-id': delivery.id,
        };
        if (endpoint.secretRef) headers['x-cajeer-signature'] = signPayload(body, endpoint.secretRef);
        const response = await fetch(endpoint.url, { method: 'POST', headers, body });
        const responseBody = await safeResponseBody(response);
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${responseBody}`);
        await this.prisma.webhookDelivery.update({
          where: { id: delivery.id },
          data: { status: 'delivered', responseStatus: response.status, responseBody, deliveredAt: new Date(), lastError: null },
        });
        results.push({ ok: true });
      } catch (error) {
        const nextAttempts = delivery.attempts;
        const dead = nextAttempts >= maxAttempts;
        const message = (error as Error).message;
        await this.prisma.webhookDelivery.update({
          where: { id: delivery.id },
          data: {
            status: dead ? 'dead' : 'failed',
            availableAt: new Date(Date.now() + Math.min(300_000, 2 ** nextAttempts * 1000)),
            lastError: message,
          },
        });
        results.push({ ok: false, error: message });
      }
    }

    const failed = results.filter((result) => !result.ok);
    if (failed.length === 0) {
      await this.prisma.outboxMessage.update({ where: { id }, data: { status: 'delivered', deliveredAt: new Date() } });
      return;
    }

    const dead = message.attempts + 1 >= maxAttempts;
    await this.prisma.outboxMessage.update({
      where: { id },
      data: {
        status: dead ? 'dead' : 'failed',
        availableAt: new Date(Date.now() + Math.min(300_000, 2 ** (message.attempts + 1) * 1000)),
        headers: { ...asRecord(message.headers), lastErrors: failed.map((item) => item.error) } as never,
      },
    });
  }
}

async function safeResponseBody(response: Response): Promise<string> {
  const text = await response.text();
  return text.slice(0, 4000);
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
}
