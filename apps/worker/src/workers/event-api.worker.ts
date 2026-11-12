import { SerialPoller } from '@cajeerbots/core';
import { PrismaService } from '@cajeerbots/db';
import { nextBackoffDate, subscriptionAllowsEvent } from '@cajeerbots/event-bus';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';
import { signPayload, safeHttpRequest } from '@cajeerbots/security';

export interface EventApiWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
}

export class EventApiDeliveryWorker {
  private poller?: SerialPoller;
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
    this.poller = new SerialPoller(this.options.pollIntervalMs ?? Number(process.env.EVENT_API_WORKER_INTERVAL_MS ?? 5000), () => this.tick(), (error) => console.error('[worker] event-api.worker tick failed:', error instanceof Error ? error.name : 'unknown'));
    await this.poller.start();
    console.log('[worker] event-api delivery worker 1.0.0-rc.3 started');
  }

  async stop(): Promise<void> {
    await this.poller?.stop();
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const rows = await this.prisma.eventSubscriptionDelivery.findMany({
        where: eventClaimable(),
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
    const lockedAt = new Date();
    const claimed = await this.prisma.eventSubscriptionDelivery.updateMany({ where: { id, ...eventClaimable() }, data: { status: 'processing', attempts: { increment: 1 }, lockedAt, lastError: null } });
    if (!claimed.count) return;
    const delivery = await this.prisma.eventSubscriptionDelivery.findUniqueOrThrow({ where: { id }, include: { subscription: true, eventRecord: true } });

    try {
      if (delivery.attempts > delivery.subscription.maxAttempts || !delivery.subscription.isActive || !subscriptionAllowsEvent(delivery.subscription.filters, delivery.eventRecord)) throw new Error('Delivery exhausted or subscription disabled');
      const result = await this.dispatch(delivery);
      await this.prisma.$transaction(async (tx) => {
        const updated = await tx.eventSubscriptionDelivery.updateMany({
          where: { id, status: 'processing', lockedAt },
          data: {
            status: 'delivered',
            deliveredAt: new Date(),
            lockedAt: null,
            responseStatus: result.responseStatus,
            responseBody: result.responseBody,
            result: result.result as never,
            lastError: null,
          },
        });
        if (updated.count) await tx.eventSubscription.update({ where: { id: delivery.subscriptionId }, data: { lastDeliveredAt: new Date() } });
      });
    } catch (error) {
      const attempts = delivery.attempts;
      const maxAttempts = delivery.subscription.maxAttempts || 8;
      const dead = attempts >= maxAttempts;
      const reason = error instanceof Error ? error.message : String(error);
      await this.prisma.$transaction(async (tx) => {
      const updated = await tx.eventSubscriptionDelivery.updateMany({
        where: { id, status: 'processing', lockedAt },
        data: {
          status: dead ? 'dead' : 'failed',
          availableAt: dead ? new Date() : nextBackoffDate(attempts),
          lockedAt: null,
          lastError: reason,
        },
      });
      if (dead && updated.count) {
        await tx.eventDeadLetter.create({
          data: {
            sourceType: 'event_subscription_delivery',
            sourceId: id,
            reason,
            payload: { event: delivery.eventRecord, subscription: delivery.subscription } as never,
            metadata: { attempts, maxAttempts } as never,
          },
        });
      }
      });
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
      const outbox = await this.prisma.outboxMessage.upsert({
        where: { id: `event-delivery-${delivery.id}` }, update: {},
        create: { id: `event-delivery-${delivery.id}`,
          topic: event.topicName,
          communityId: event.communityId,
          payload: envelope as never,
          headers: { source: 'event_api_subscription', subscriptionId: subscription.id, eventRecordId: event.id, targetRef: subscription.targetRef } as never,
        },
      });
      return { result: { target: 'outbox', outboxId: outbox.id } };
    }

    if (subscription.targetType === 'extension_hook') {
      const queued = await this.extensionRuntime.enqueueHook({ hook: subscription.targetRef, payload: envelope, idempotencyKey: delivery.id });
      return { result: { target: 'extension_hook', ...queued } };
    }

    if (subscription.targetType === 'webhook_endpoint') {
      const endpoint = await this.prisma.webhookEndpoint.findUnique({ where: { id: subscription.targetRef } });
      if (!endpoint || !endpoint.isActive || endpoint.communityId !== event.communityId) throw new Error(`Webhook endpoint ${subscription.targetRef} не найден или отключён`);
      const body = JSON.stringify(envelope);
      const headers: Record<string, string> = {
        'content-type': 'application/json',
        'x-cajeer-event': event.topicName,
        'x-cajeer-event-record-id': event.id,
        'x-cajeer-event-delivery-id': delivery.id,
        'x-cajeer-subscription-id': subscription.id,
      };
      if (endpoint.secretRef) headers['x-cajeer-signature'] = signPayload(body, endpoint.secretRef);
      const response = await safeHttpRequest(endpoint.url, { method: 'POST', headers, body });
      const responseBody = (await response.text()).slice(0, 4000);
      if (!response.ok) throw new Error(`HTTP ${response.status}: ${responseBody}`);
      return { responseStatus: response.status, responseBody, result: { target: 'webhook_endpoint', endpointId: endpoint.id } };
    }

    throw new Error(`Неподдерживаемый targetType EventSubscription: ${subscription.targetType}`);
  }
}

type DeliveryWithRelations = Awaited<ReturnType<PrismaService['eventSubscriptionDelivery']['findFirst']>> & {
  subscription: { id: string; targetType: string; targetRef: string; maxAttempts: number };
  eventRecord: { communityId: string | null; id: string; eventId: string; topicName: string; type: string; source: string; payload: unknown; metadata: unknown; publishedAt: Date };
};

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function eventClaimable() {
  const now = new Date();
  return { OR: [{ status: { in: ['pending', 'failed'] as ('pending' | 'failed')[] }, availableAt: { lte: now } }, { status: 'processing' as const, OR: [{ lockedAt: { lt: new Date(now.getTime() - 300_000) } }, { lockedAt: null }] }] };
}
