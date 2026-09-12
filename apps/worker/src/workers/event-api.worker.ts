import { PrismaService } from '@cajeerbots/db';
import { nextBackoffDate } from '@cajeerbots/event-bus';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';
import { signPayload } from '@cajeerbots/security';

export interface EventApiWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
}

export class EventApiDeliveryWorker {
  private timer?: NodeJS.Timeout;
  private running = false;
  private readonly extensionRuntime: DbExtensionRuntime;

  constructor(
    private readonly prisma = new PrismaService(),
    private readonly options: EventApiWorkerOptions = {},
  ) {
    this.extensionRuntime = new DbExtensionRuntime(prisma);
  }

  async start(): Promise<void> {
    await this.prisma.connect();
    await this.tick();
    this.timer = setInterval(() => void this.tick(), this.options.pollIntervalMs ?? Number(process.env.EVENT_API_WORKER_INTERVAL_MS ?? 5000));
    console.log('[worker] event-api delivery worker 1.0.0-rc.1 started');
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const rows = await this.prisma.eventSubscriptionDelivery.findMany({
        where: { status: { in: ['pending', 'failed'] }, availableAt: { lte: new Date() } },
        orderBy: { createdAt: 'asc' },
        take: this.options.batchSize ?? Number(process.env.EVENT_API_WORKER_BATCH_SIZE ?? 25),
        include: { subscription: true, eventRecord: true },
      });
      for (const row of rows) await this.deliver(row.id);
    } finally {
      this.running = false;
    }
  }

  async deliver(id: string): Promise<void> {
    const delivery = await this.prisma.eventSubscriptionDelivery.update({
      where: { id },
      data: { status: 'processing', attempts: { increment: 1 }, lockedAt: new Date(), lastError: null },
      include: { subscription: true, eventRecord: true },
    });

    try {
      const result = await this.dispatch(delivery);
      await this.prisma.$transaction([
        this.prisma.eventSubscriptionDelivery.update({
          where: { id },
          data: {
            status: 'delivered',
            deliveredAt: new Date(),
            lockedAt: null,
            responseStatus: result.responseStatus,
            responseBody: result.responseBody,
            result: result.result as never,
            lastError: null,
          },
        }),
        this.prisma.eventSubscription.update({ where: { id: delivery.subscriptionId }, data: { lastDeliveredAt: new Date() } }),
      ]);
    } catch (error) {
      const attempts = delivery.attempts + 1;
      const maxAttempts = delivery.subscription.maxAttempts || 8;
      const dead = attempts >= maxAttempts;
      const reason = error instanceof Error ? error.message : String(error);
      await this.prisma.eventSubscriptionDelivery.update({
        where: { id },
        data: {
          status: dead ? 'dead' : 'failed',
          availableAt: dead ? new Date() : nextBackoffDate(attempts),
          lockedAt: null,
          lastError: reason,
        },
      });
      if (dead) {
        await this.prisma.eventDeadLetter.create({
          data: {
            sourceType: 'event_subscription_delivery',
            sourceId: id,
            reason,
            payload: { event: delivery.eventRecord, subscription: delivery.subscription } as never,
            metadata: { attempts, maxAttempts } as never,
          },
        });
      }
    }
  }

  private async dispatch(delivery: DeliveryWithRelations): Promise<{ responseStatus?: number; responseBody?: string; result?: Record<string, unknown> }> {
    const subscription = delivery.subscription;
    const event = delivery.eventRecord;
    const envelope = {
      id: event.eventId,
      type: event.type,
      occurredAt: event.publishedAt.toISOString(),
      payload: event.payload,
      metadata: { ...(asRecord(event.metadata)), topic: event.topicName, source: event.source, eventRecordId: event.id },
    };

    if (subscription.targetType === 'log') {
      return { result: { target: 'log', eventRecordId: event.id, topic: event.topicName } };
    }

    if (subscription.targetType === 'outbox') {
      const outbox = await this.prisma.outboxMessage.create({
        data: {
          topic: event.topicName,
          payload: envelope as never,
          headers: { source: 'event_api_subscription', subscriptionId: subscription.id, eventRecordId: event.id, targetRef: subscription.targetRef } as never,
        },
      });
      return { result: { target: 'outbox', outboxId: outbox.id } };
    }

    if (subscription.targetType === 'extension_hook') {
      const queued = await this.extensionRuntime.enqueueHook({ hook: subscription.targetRef, payload: envelope });
      return { result: { target: 'extension_hook', ...queued } };
    }

    if (subscription.targetType === 'webhook_endpoint') {
      const endpoint = await this.prisma.webhookEndpoint.findUnique({ where: { id: subscription.targetRef } });
      if (!endpoint || !endpoint.isActive) throw new Error(`Webhook endpoint ${subscription.targetRef} не найден или отключён`);
      const body = JSON.stringify(envelope);
      const headers: Record<string, string> = {
        'content-type': 'application/json',
        'x-cajeer-event': event.topicName,
        'x-cajeer-event-record-id': event.id,
        'x-cajeer-event-delivery-id': delivery.id,
        'x-cajeer-subscription-id': subscription.id,
      };
      if (endpoint.secretRef) headers['x-cajeer-signature'] = signPayload(body, endpoint.secretRef);
      const response = await fetch(endpoint.url, { method: 'POST', headers, body });
      const responseBody = (await response.text()).slice(0, 4000);
      if (!response.ok) throw new Error(`HTTP ${response.status}: ${responseBody}`);
      return { responseStatus: response.status, responseBody, result: { target: 'webhook_endpoint', endpointId: endpoint.id } };
    }

    throw new Error(`Неподдерживаемый targetType EventSubscription: ${subscription.targetType}`);
  }
}

type DeliveryWithRelations = Awaited<ReturnType<PrismaService['eventSubscriptionDelivery']['findFirst']>> & {
  subscription: { id: string; targetType: string; targetRef: string; maxAttempts: number };
  eventRecord: { id: string; eventId: string; topicName: string; type: string; source: string; payload: unknown; metadata: unknown; publishedAt: Date };
};

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
}
