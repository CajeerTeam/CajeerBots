import { isDeepStrictEqual } from 'node:util';
import { BadRequestException, ConflictException, Injectable, UnauthorizedException } from '@nestjs/common';
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
    if (!secret || !signature || !verifyPayloadSignature(rawBody, secret, signature)) throw new UnauthorizedException('Некорректная подпись webhook payload');
    const key = `incoming:${sha256(idempotencyKey ?? event.id)}`;
    const requestHash = sha256(rawBody);
    const metadata = { ...event.metadata };
    delete metadata.communityId;
    const normalized: EventEnvelope = { ...event, metadata };
    const result = await this.prisma.$transaction(async (tx) => {
      const reservation = await tx.idempotencyKey.upsert({ where: { key }, create: { key, scope: 'incoming.webhook', requestHash, expiresAt: new Date(Date.now() + 86400000) }, update: {} });
      if (reservation.requestHash !== requestHash) throw new ConflictException('Idempotency key reused with another payload');
      if (reservation.responseBody) return { ...(reservation.responseBody as Record<string, unknown>), duplicate: true };
      const topic = await tx.eventTopic.upsert({ where: { name: event.type }, create: { name: event.type }, update: {} });
      if (!topic.isActive) throw new BadRequestException('Topic disabled');
      const record = await tx.eventRecord.upsert({ where: { eventId: event.id }, create: { eventId: event.id, topicId: topic.id, topicName: event.type, type: event.type, source: 'incoming.webhook', payload: normalized as never, metadata: normalized.metadata as never }, update: {} });
      if (record.source !== 'incoming.webhook' || record.communityId !== null || record.type !== event.type || !isDeepStrictEqual((record.payload as { payload?: unknown }).payload, event.payload)) throw new ConflictException('Event ID already exists');
      const outbox = await tx.outboxMessage.upsert({ where: { id: record.outboxMessageId ?? `incoming-${record.id}` }, create: { id: record.outboxMessageId ?? `incoming-${record.id}`, topic: record.topicName, payload: record.payload as never, headers: { source: 'incoming.webhook' } }, update: {} });
      await tx.eventRecord.update({ where: { id: record.id }, data: { outboxMessageId: outbox.id } });
      const subscriptions = await tx.eventSubscription.findMany({ where: { isActive: true } });
      let deliveries = 0;
      for (const subscription of subscriptions) {
        if (!matchesTopicPattern(subscription.topicPattern, record.topicName) || !subscriptionAllowsEvent(subscription.filters, record)) continue;
        await tx.eventSubscriptionDelivery.upsert({ where: { subscriptionId_eventRecordId: { subscriptionId: subscription.id, eventRecordId: record.id } }, create: { subscriptionId: subscription.id, eventRecordId: record.id }, update: {} });
        deliveries++;
      }
      const response = { accepted: true, duplicate: false, eventId: event.id, outboxId: outbox.id, eventRecordId: record.id, eventDeliveries: deliveries, signature: 'verified' };
      await tx.idempotencyKey.update({ where: { key }, data: { responseBody: response } });
      await tx.auditLog.create({ data: { actorType: 'system', action: 'webhook.receive', resource: 'outbox_message', resourceId: outbox.id, metadata: response } });
      return response;
    });
    await this.extensionRuntime.enqueueHook({ hook: 'webhook.received', payload: normalized, idempotencyKey: event.id });
    return result;
  }

  signExample(body: unknown) {
    const secret = process.env.WEBHOOK_SIGNING_SECRET;
    if (!secret) throw new BadRequestException('WEBHOOK_SIGNING_SECRET не задан');
    const payload = JSON.stringify(body);
    return { payload, signature: signPayload(payload, secret) };
  }
}

