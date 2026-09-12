import { BadRequestException, Injectable, UnauthorizedException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { signPayload, sha256, verifyPayloadSignature } from '@cajeerbots/security';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';
import { matchesTopicPattern, subscriptionAllowsEvent } from '@cajeerbots/event-bus';
import type { EventEnvelope } from '@cajeerbots/core';

@Injectable()
export class WebhooksService {
  private readonly extensionRuntime: DbExtensionRuntime;

  constructor(private readonly prisma: PrismaService) {
    this.extensionRuntime = new DbExtensionRuntime(prisma);
  }

  async receive(event: EventEnvelope, rawBody: string, signature?: string, idempotencyKey?: string) {
    const secret = process.env.WEBHOOK_SIGNING_SECRET;
    if (secret) {
      if (!signature || !verifyPayloadSignature(rawBody, secret, signature)) {
        throw new UnauthorizedException('Некорректная подпись webhook payload');
      }
    }

    const key = idempotencyKey ?? `event:${event.id}`;
    const requestHash = sha256(rawBody);
    try {
      await this.prisma.idempotencyKey.create({
        data: {
          key,
          scope: 'incoming.webhook',
          requestHash,
          expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
          responseBody: { accepted: true, duplicate: false } as never,
        },
      });
    } catch (error) {
      if (isUniqueError(error)) return { accepted: true, duplicate: true, eventId: event.id };
      throw error;
    }

    const normalized: EventEnvelope = {
      id: event.id,
      type: event.type,
      occurredAt: event.occurredAt ?? new Date().toISOString(),
      payload: event.payload,
      metadata: event.metadata ?? {},
    };
    const outbox = await this.prisma.outboxMessage.create({ data: { topic: normalized.type, payload: normalized as never, headers: { source: 'incoming.webhook' } as never } });
    const eventRecord = await this.recordDurableEvent(normalized, outbox.id);
    const extensionEvents = await this.extensionRuntime.enqueueHook({ hook: 'webhook.received', payload: normalized });
    await this.prisma.auditLog.create({ data: { actorType: 'system', action: 'webhook.receive', resource: 'outbox_message', resourceId: outbox.id, metadata: { eventId: event.id, type: event.type, eventRecordId: eventRecord.id, eventDeliveries: eventRecord.deliveries, extensionEvents } } });
    return { accepted: true, duplicate: false, eventId: event.id, outboxId: outbox.id, eventRecordId: eventRecord.id, eventDeliveries: eventRecord.deliveries, extensionEvents, signature: secret ? 'verified' : signature ? 'present' : 'not_required' };
  }


  private async recordDurableEvent(event: EventEnvelope, outboxId: string): Promise<{ id: string; deliveries: number }> {
    const topic = await this.prisma.eventTopic.upsert({
      where: { name: event.type },
      create: { name: event.type, description: `Автоматически созданный topic ${event.type}` },
      update: {},
    });
    const record = await this.prisma.eventRecord.upsert({
      where: { eventId: event.id },
      create: {
        eventId: event.id,
        topicId: topic.id,
        topicName: event.type,
        type: event.type,
        source: 'incoming.webhook',
        payload: event as never,
        metadata: event.metadata as never,
        headers: { source: 'incoming.webhook' } as never,
        outboxMessageId: outboxId,
      },
      update: { outboxMessageId: outboxId },
    });
    const subscriptions = await this.prisma.eventSubscription.findMany({ where: { isActive: true } });
    let deliveries = 0;
    for (const subscription of subscriptions) {
      if (!matchesTopicPattern(subscription.topicPattern, record.topicName)) continue;
      if (!subscriptionAllowsEvent(subscription.filters, record)) continue;
      await this.prisma.eventSubscriptionDelivery.upsert({
        where: { subscriptionId_eventRecordId: { subscriptionId: subscription.id, eventRecordId: record.id } },
        create: { subscriptionId: subscription.id, eventRecordId: record.id },
        update: {},
      });
      deliveries += 1;
    }
    return { id: record.id, deliveries };
  }

  signExample(body: unknown) {
    const secret = process.env.WEBHOOK_SIGNING_SECRET;
    if (!secret) throw new BadRequestException('WEBHOOK_SIGNING_SECRET не задан');
    const payload = JSON.stringify(body);
    return { payload, signature: signPayload(payload, secret) };
  }
}

function isUniqueError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === 'P2002';
}
