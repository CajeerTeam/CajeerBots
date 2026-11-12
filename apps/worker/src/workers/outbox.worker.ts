import { SerialPoller } from '@cajeerbots/core';
import { PrismaService } from '@cajeerbots/db';
import { signPayload, safeHttpRequest } from '@cajeerbots/security';

export interface OutboxWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
}

export class OutboxWorker {
  private poller?: SerialPoller;
  private running = false;

  constructor(
    private readonly prisma = new PrismaService(),
    private readonly options: OutboxWorkerOptions = {},
  ) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    this.poller = new SerialPoller(this.options.pollIntervalMs ?? 5000, () => this.tick(), (error) => console.error('[worker] outbox.worker tick failed:', error instanceof Error ? error.name : 'unknown'));
    await this.poller.start();
    console.log('[worker] outbox worker 1.0.0 started');
  }

  async stop(): Promise<void> {
    await this.poller?.stop();
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const messages = await this.prisma.outboxMessage.findMany({
        where: { status: { in: ['pending', 'failed', 'processing'] }, availableAt: { lte: new Date() } },
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
    const lease = new Date(Date.now() + 300_000);
    const claimed = await this.prisma.outboxMessage.updateMany({ where: { id, status: { in: ['pending', 'failed', 'processing'] }, availableAt: { lte: new Date() } }, data: { status: 'processing', availableAt: lease, attempts: { increment: 1 } } });
    if (!claimed.count) return;
    const message = await this.prisma.outboxMessage.findUniqueOrThrow({ where: { id } });
    // Materialize the delivery set once. A retry never re-sends a successful endpoint.
    const existing = await this.prisma.webhookDelivery.count({ where: { outboxMessageId: id } });
    if (!existing) {
      const endpoints = await this.prisma.webhookEndpoint.findMany({ where: { isActive: true, communityId: message.communityId, OR: [{ events: { has: message.topic } }, { events: { has: '*' } }] } });
      await this.prisma.webhookDelivery.createMany({ data: endpoints.map((endpoint) => ({ outboxMessageId: id, endpointId: endpoint.id })), skipDuplicates: true });
    }
    const deliveries = await this.prisma.webhookDelivery.findMany({ where: { outboxMessageId: id, status: { in: ['pending', 'failed', 'processing'] }, availableAt: { lte: new Date() } }, include: { endpoint: true } });
    for (const delivery of deliveries) {
      const renewed = await this.prisma.outboxMessage.updateMany({ where: { id, status: 'processing', attempts: message.attempts }, data: { availableAt: new Date(Date.now() + 300_000) } });
      if (!renewed.count) return;
      const claim = await this.prisma.webhookDelivery.updateMany({ where: { id: delivery.id, status: { in: ['pending', 'failed', 'processing'] }, availableAt: { lte: new Date() } }, data: { status: 'processing', attempts: { increment: 1 }, availableAt: new Date(Date.now() + 300_000) } });
      if (!claim.count) continue;
      const attempts = delivery.attempts + 1;
      const where = { id: delivery.id, status: 'processing' as const, attempts };
      try {
        if (!delivery.endpoint.isActive || delivery.endpoint.communityId !== message.communityId || attempts > maxAttempts) throw new Error('Endpoint disabled or attempts exhausted');
        const body = JSON.stringify(message.payload);
        const headers: Record<string, string> = { 'content-type': 'application/json', 'x-cajeer-event': message.topic, 'x-cajeer-outbox-id': id, 'x-cajeer-delivery-id': delivery.id };
        if (delivery.endpoint.secretRef) headers['x-cajeer-signature'] = signPayload(body, delivery.endpoint.secretRef);
        const response = await safeHttpRequest(delivery.endpoint.url, { method: 'POST', headers, body });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        await this.prisma.webhookDelivery.updateMany({ where, data: { status: 'delivered', responseStatus: response.status, responseBody: await response.text(), deliveredAt: new Date(), lastError: null } });
      } catch (error) {
        await this.prisma.webhookDelivery.updateMany({ where, data: { status: attempts >= maxAttempts ? 'dead' : 'failed', lastError: error instanceof Error ? error.message : 'Delivery failed', availableAt: new Date(Date.now() + Math.min(300_000, 2 ** attempts * 1000)) } });
      }
    }
    const remaining = await this.prisma.webhookDelivery.findMany({ where: { outboxMessageId: id, status: { not: 'delivered' } }, orderBy: { availableAt: 'asc' } });
    const retry = remaining.find((delivery) => delivery.status !== 'dead');
    await this.prisma.outboxMessage.updateMany({ where: { id, status: 'processing', attempts: message.attempts }, data: { status: retry ? 'failed' : remaining.length ? 'dead' : 'delivered', availableAt: retry?.availableAt ?? new Date(), deliveredAt: remaining.length ? null : new Date() } });
  }
}

