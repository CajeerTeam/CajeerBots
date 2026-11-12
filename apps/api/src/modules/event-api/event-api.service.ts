import { isDeepStrictEqual } from 'node:util';
import { BadRequestException, ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { createDurableEventEnvelope, eventFingerprint, matchesTopicPattern, subscriptionAllowsEvent } from '@cajeerbots/event-bus';
import type { Principal } from '@cajeerbots/security';

interface PublishInput {
  id?: string;
  topic?: string;
  type: string;
  source?: string;
  communityId?: string;
  aggregateType?: string;
  aggregateId?: string;
  payload: unknown;
  metadata?: Record<string, unknown>;
  headers?: Record<string, unknown>;
  fanoutToOutbox?: boolean;
}

interface ListEventsQuery {
  topic?: string;
  type?: string;
  communityId?: string;
  aggregateType?: string;
  aggregateId?: string;
  source?: string;
  take?: number;
}

@Injectable()
export class EventApiService {
  constructor(private readonly prisma: PrismaService) {}

  topics() {
    return this.prisma.eventTopic.findMany({ orderBy: { name: 'asc' }, include: { _count: { select: { records: true, subscriptions: true } } } });
  }

  upsertTopic(name: string, input: { description?: string; schema?: Record<string, unknown>; isActive?: boolean; retentionDays?: number }, actor: Principal) {
    return this.prisma.$transaction(async (tx) => {
      const topic = await tx.eventTopic.upsert({
        where: { name },
        create: {
          name,
          description: input.description,
          schema: input.schema as never,
          isActive: input.isActive ?? true,
          retentionDays: input.retentionDays ?? 30,
        },
        update: {
          description: input.description,
          schema: input.schema as never,
          isActive: input.isActive,
          retentionDays: input.retentionDays,
        },
      });
      await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'event.topic.upsert', resource: 'event_topic', resourceId: topic.id, metadata: { name } } });
      return topic;
    });
  }

  async publish(input: PublishInput, actor: Principal | { id?: string; type: string } = { type: 'system' }) {
    const topicName = input.topic ?? input.type;
    const topic = await this.prisma.eventTopic.upsert({
      where: { name: topicName },
      create: { name: topicName, description: `Автоматически созданный topic ${topicName}` },
      update: {},
    });
    if (!topic.isActive) throw new BadRequestException(`Topic ${topicName} отключён`);

    if (input.communityId && !await this.prisma.community.findUnique({ where: { id: input.communityId } })) throw new NotFoundException('Community not found');
    const eventId = input.id ?? eventFingerprint({ ...input, topic: topicName, source: input.source ?? 'api' });
    const envelope = createDurableEventEnvelope({
      id: eventId,
      type: input.type,
      topic: topicName,
      source: input.source ?? 'api',
      communityId: input.communityId,
      aggregateType: input.aggregateType,
      aggregateId: input.aggregateId,
      payload: input.payload,
      metadata: input.metadata,
      headers: input.headers,
    });
    return this.prisma.$transaction(async (tx) => {
      const record = await tx.eventRecord.upsert({
        where: { eventId },
        create: {
          eventId,
          topicId: topic.id,
          topicName,
          type: input.type,
          source: input.source ?? 'api',
          communityId: input.communityId,
          aggregateType: input.aggregateType,
          aggregateId: input.aggregateId,
          payload: envelope as never,
          metadata: envelope.metadata as never,
          headers: input.headers as never,
        },
        update: {},
      });

      if (record.communityId !== (input.communityId ?? null) || record.topicName !== topicName || record.type !== input.type || record.source !== (input.source ?? 'api') || record.aggregateType !== (input.aggregateType ?? null) || record.aggregateId !== (input.aggregateId ?? null) || !isDeepStrictEqual((record.payload as { payload?: unknown }).payload, input.payload)) throw new ConflictException('Event ID already belongs to another event');
      const subscriptions = await tx.eventSubscription.findMany({ where: { isActive: true } });
      const matched = subscriptions.filter((subscription) => matchesTopicPattern(subscription.topicPattern, topicName) && subscriptionAllowsEvent(subscription.filters, record));
      for (const subscription of matched) {
        await tx.eventSubscriptionDelivery.upsert({
          where: { subscriptionId_eventRecordId: { subscriptionId: subscription.id, eventRecordId: record.id } },
          create: { subscriptionId: subscription.id, eventRecordId: record.id },
          update: {},
        });
      }

      let outboxId: string | null = null;
      if (input.fanoutToOutbox) {
        const outbox = await tx.outboxMessage.upsert({ where: { id: record.outboxMessageId ?? `event-${record.id}` }, update: {}, create: { id: record.outboxMessageId ?? `event-${record.id}`, topic: topicName, communityId: record.communityId, payload: record.payload as never, headers: { ...(input.headers ?? {}), source: 'event_api', eventRecordId: record.id } as never } });
        outboxId = outbox.id;
        await tx.eventRecord.update({ where: { id: record.id }, data: { outboxMessageId: outbox.id } });
      }

      await tx.auditLog.create({
        data: {
          actorId: actor.id,
          actorType: actor.type,
          action: 'event.publish',
          resource: 'event_record',
          resourceId: record.id,
          metadata: { topic: topicName, type: input.type, matchedSubscriptions: matched.length, outboxId },
        },
      });

      return { accepted: true, event: record, matchedSubscriptions: matched.length, outboxId };
    });
  }

  listEvents(query: ListEventsQuery) {
    return this.prisma.eventRecord.findMany({
      where: {
        ...(query.topic ? { topicName: query.topic } : {}),
        ...(query.type ? { type: query.type } : {}),
        ...(query.communityId ? { communityId: query.communityId } : {}),
        ...(query.aggregateType ? { aggregateType: query.aggregateType } : {}),
        ...(query.aggregateId ? { aggregateId: query.aggregateId } : {}),
        ...(query.source ? { source: query.source } : {}),
      },
      orderBy: { publishedAt: 'desc' },
      take: Math.min(Math.max(query.take ?? 50, 1), 500),
      include: { deliveries: { include: { subscription: true } } },
    });
  }

  async getEvent(id: string) {
    const event = await this.prisma.eventRecord.findUnique({ where: { id }, include: { deliveries: { include: { subscription: true } } } });
    if (!event) throw new NotFoundException('Event record не найден');
    return event;
  }

  async replayEvent(id: string, actor: Principal) {
    const event = await this.getEvent(id);
    const subscriptions = await this.prisma.eventSubscription.findMany({ where: { isActive: true } });
    const matched = subscriptions.filter((subscription) => matchesTopicPattern(subscription.topicPattern, event.topicName) && subscriptionAllowsEvent(subscription.filters, event));
    await this.prisma.$transaction(async (tx) => {
      for (const subscription of matched) {
        await tx.eventSubscriptionDelivery.upsert({
          where: { subscriptionId_eventRecordId: { subscriptionId: subscription.id, eventRecordId: event.id } },
          create: { subscriptionId: subscription.id, eventRecordId: event.id, status: 'pending', availableAt: new Date() },
          update: {},
        });
      }
      await tx.eventRecord.update({ where: { id }, data: { replayCount: { increment: 1 }, replayedAt: new Date() } });
      await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'event.replay', resource: 'event_record', resourceId: id, metadata: { matchedSubscriptions: matched.length } } });
    });
    return { replayed: true, eventId: id, matchedSubscriptions: matched.length };
  }

  subscriptions() {
    return this.prisma.eventSubscription.findMany({ orderBy: { createdAt: 'desc' }, include: { _count: { select: { deliveries: true } } } });
  }

  async createSubscription(input: { name: string; topicPattern: string; targetType: string; targetRef: string; filters?: Record<string, unknown>; isActive?: boolean; maxAttempts?: number }, actor: Principal) {
    const topic = await this.prisma.eventTopic.findUnique({ where: { name: input.topicPattern } }).catch(() => null);
    const subscription = await this.prisma.eventSubscription.create({
      data: {
        name: input.name,
        topicPattern: input.topicPattern,
        topicId: topic?.id,
        targetType: input.targetType,
        targetRef: input.targetRef,
        filters: input.filters as never,
        isActive: input.isActive ?? true,
        maxAttempts: input.maxAttempts ?? 8,
      },
    });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'event.subscription.create', resource: 'event_subscription', resourceId: subscription.id, metadata: { name: input.name } } });
    return subscription;
  }

  async updateSubscription(id: string, input: { topicPattern?: string; targetType?: string; targetRef?: string; filters?: Record<string, unknown>; isActive?: boolean; maxAttempts?: number }, actor: Principal) {
    await this.requireSubscription(id);
    const topic = input.topicPattern ? await this.prisma.eventTopic.findUnique({ where: { name: input.topicPattern } }) : null;
    const subscription = await this.prisma.eventSubscription.update({
      where: { id },
      data: {
        topicPattern: input.topicPattern,
        topicId: topic?.id,
        targetType: input.targetType,
        targetRef: input.targetRef,
        filters: input.filters as never,
        isActive: input.isActive,
        maxAttempts: input.maxAttempts,
        disabledAt: input.isActive === false ? new Date() : input.isActive === true ? null : undefined,
      },
    });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'event.subscription.update', resource: 'event_subscription', resourceId: id } });
    return subscription;
  }

  async setSubscriptionState(id: string, enabled: boolean, actor: Principal) {
    await this.requireSubscription(id);
    const subscription = await this.prisma.eventSubscription.update({ where: { id }, data: { isActive: enabled, disabledAt: enabled ? null : new Date() } });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: enabled ? 'event.subscription.enable' : 'event.subscription.disable', resource: 'event_subscription', resourceId: id } });
    return subscription;
  }

  deliveries(subscriptionId?: string, status?: string, take = 50) {
    return this.prisma.eventSubscriptionDelivery.findMany({
      where: { ...(subscriptionId ? { subscriptionId } : {}), ...(status ? { status: status as never } : {}) },
      orderBy: { createdAt: 'desc' },
      take: Math.min(Math.max(take, 1), 500),
      include: { subscription: true, eventRecord: true },
    });
  }

  async retryDelivery(id: string, resetAttempts: boolean, actor: Principal) {
    const delivery = await this.prisma.eventSubscriptionDelivery.findUnique({ where: { id } });
    if (!delivery) throw new NotFoundException('Delivery не найден');
    const changed = await this.prisma.eventSubscriptionDelivery.updateMany({ where: { id, status: { in: ['failed', 'dead'] } }, data: { status: 'pending', availableAt: new Date(), lockedAt: null, lastError: null, ...(resetAttempts ? { attempts: 0 } : {}) } });
    if (!changed.count) throw new ConflictException('Only failed or dead deliveries can be retried');
    const updated = await this.prisma.eventSubscriptionDelivery.findUniqueOrThrow({ where: { id } });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'event.delivery.retry', resource: 'event_subscription_delivery', resourceId: id } });
    return updated;
  }

  deadLetters(status?: string, take = 50) {
    return this.prisma.eventDeadLetter.findMany({ where: status ? { status } : {}, orderBy: { createdAt: 'desc' }, take: Math.min(Math.max(take, 1), 500) });
  }

  async resolveDeadLetter(id: string, actor: Principal) {
    const updated = await this.prisma.eventDeadLetter.update({ where: { id }, data: { status: 'resolved', resolvedAt: new Date(), resolvedBy: actor.id } });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'event.dead_letter.resolve', resource: 'event_dead_letter', resourceId: id } });
    return updated;
  }

  private async requireSubscription(id: string) {
    const subscription = await this.prisma.eventSubscription.findUnique({ where: { id } });
    if (!subscription) throw new NotFoundException('Event subscription не найдена');
    return subscription;
  }
}
