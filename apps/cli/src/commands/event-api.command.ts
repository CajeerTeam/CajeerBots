import { readFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { createDurableEventEnvelope, eventFingerprint, matchesTopicPattern, subscriptionAllowsEvent } from '@cajeerbots/event-bus';

export function eventApiCommand(): Command {
  const command = new Command('events').description('Event API: topics, durable journal, subscriptions, replay, dead-letter');

  command.command('topics:list')
    .description('Показать topics Event API')
    .action(async () => withPrisma(async (prisma) => {
      console.log(JSON.stringify(await prisma.eventTopic.findMany({ orderBy: { name: 'asc' } }), null, 2));
    }));

  command.command('topic:set')
    .requiredOption('--name <name>', 'topic name')
    .option('--description <text>', 'описание')
    .option('--retention-days <n>', 'retention days', '30')
    .option('--disabled', 'создать/обновить topic отключённым')
    .action(async (options: { name: string; description?: string; retentionDays: string; disabled?: boolean }) => withPrisma(async (prisma) => {
      const topic = await prisma.eventTopic.upsert({
        where: { name: options.name },
        create: { name: options.name, description: options.description, retentionDays: Number(options.retentionDays), isActive: !options.disabled },
        update: { description: options.description, retentionDays: Number(options.retentionDays), isActive: !options.disabled },
      });
      console.log(JSON.stringify(topic, null, 2));
    }));

  command.command('publish')
    .requiredOption('--type <type>', 'event type')
    .option('--topic <topic>', 'topic name')
    .option('--source <source>', 'source', 'cli')
    .option('--community <id>', 'community id')
    .option('--aggregate-type <type>', 'aggregate type')
    .option('--aggregate-id <id>', 'aggregate id')
    .option('--payload <json>', 'JSON payload string')
    .option('--payload-file <path>', 'JSON payload file')
    .option('--fanout-outbox', 'создать outbox message вместе с event record')
    .action(async (options: { type: string; topic?: string; source: string; community?: string; aggregateType?: string; aggregateId?: string; payload?: string; payloadFile?: string; fanoutOutbox?: boolean }) => withPrisma(async (prisma) => {
      const payload = await readJsonInput(options.payload, options.payloadFile);
      const topicName = options.topic ?? options.type;
      const topic = await prisma.eventTopic.upsert({ where: { name: topicName }, create: { name: topicName }, update: {} });
      const envelope = createDurableEventEnvelope({ type: options.type, topic: topicName, source: options.source, communityId: options.community, aggregateType: options.aggregateType, aggregateId: options.aggregateId, payload });
      const eventId = eventFingerprint({ type: options.type, topic: topicName, source: options.source, communityId: options.community, aggregateType: options.aggregateType, aggregateId: options.aggregateId, payload });
      const record = await prisma.eventRecord.upsert({
        where: { eventId },
        create: { eventId, topicId: topic.id, topicName, type: options.type, source: options.source, communityId: options.community, aggregateType: options.aggregateType, aggregateId: options.aggregateId, payload: envelope as never, metadata: envelope.metadata as never },
        update: {},
      });
      const subscriptions = await prisma.eventSubscription.findMany({ where: { isActive: true } });
      let matched = 0;
      for (const subscription of subscriptions) {
        if (!matchesTopicPattern(subscription.topicPattern, topicName) || !subscriptionAllowsEvent(subscription.filters, record)) continue;
        await prisma.eventSubscriptionDelivery.upsert({ where: { subscriptionId_eventRecordId: { subscriptionId: subscription.id, eventRecordId: record.id } }, create: { subscriptionId: subscription.id, eventRecordId: record.id }, update: { status: 'pending', availableAt: new Date(), lastError: null } });
        matched += 1;
      }
      let outboxId: string | undefined;
      if (options.fanoutOutbox) {
        const outbox = await prisma.outboxMessage.create({ data: { topic: topicName, payload: envelope as never, headers: { source: 'event_api_cli', eventRecordId: record.id } as never } });
        outboxId = outbox.id;
        await prisma.eventRecord.update({ where: { id: record.id }, data: { outboxMessageId: outbox.id } });
      }
      console.log(JSON.stringify({ event: record, matchedSubscriptions: matched, outboxId }, null, 2));
    }));

  command.command('records:list')
    .option('--topic <topic>', 'topic')
    .option('--type <type>', 'type')
    .option('--community <id>', 'community id')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { topic?: string; type?: string; community?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.eventRecord.findMany({ where: { ...(options.topic ? { topicName: options.topic } : {}), ...(options.type ? { type: options.type } : {}), ...(options.community ? { communityId: options.community } : {}) }, orderBy: { publishedAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('record:replay')
    .requiredOption('--id <id>', 'event record id')
    .action(async (options: { id: string }) => withPrisma(async (prisma) => {
      const record = await prisma.eventRecord.findUniqueOrThrow({ where: { id: options.id } });
      const subscriptions = await prisma.eventSubscription.findMany({ where: { isActive: true } });
      let matched = 0;
      for (const subscription of subscriptions) {
        if (!matchesTopicPattern(subscription.topicPattern, record.topicName) || !subscriptionAllowsEvent(subscription.filters, record)) continue;
        await prisma.eventSubscriptionDelivery.upsert({ where: { subscriptionId_eventRecordId: { subscriptionId: subscription.id, eventRecordId: record.id } }, create: { subscriptionId: subscription.id, eventRecordId: record.id }, update: { status: 'pending', availableAt: new Date(), lockedAt: null, lastError: null } });
        matched += 1;
      }
      await prisma.eventRecord.update({ where: { id: options.id }, data: { replayCount: { increment: 1 }, replayedAt: new Date() } });
      console.log(JSON.stringify({ replayed: true, matchedSubscriptions: matched }, null, 2));
    }));

  command.command('subscriptions:list')
    .action(async () => withPrisma(async (prisma) => {
      console.log(JSON.stringify(await prisma.eventSubscription.findMany({ orderBy: { createdAt: 'desc' } }), null, 2));
    }));

  command.command('subscription:create')
    .requiredOption('--name <name>', 'subscription name')
    .requiredOption('--topic <pattern>', 'topic pattern, e.g. *, bot.*, system.audit')
    .requiredOption('--target-type <type>', 'outbox | webhook_endpoint | extension_hook | log')
    .requiredOption('--target-ref <ref>', 'target reference')
    .option('--filters <json>', 'filters JSON')
    .option('--max-attempts <n>', 'max attempts', '8')
    .action(async (options: { name: string; topic: string; targetType: string; targetRef: string; filters?: string; maxAttempts: string }) => withPrisma(async (prisma) => {
      const row = await prisma.eventSubscription.create({ data: { name: options.name, topicPattern: options.topic, targetType: options.targetType, targetRef: options.targetRef, filters: options.filters ? JSON.parse(options.filters) as never : undefined, maxAttempts: Number(options.maxAttempts) } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('deliveries:list')
    .option('--subscription <id>', 'subscription id')
    .option('--status <status>', 'pending | processing | delivered | failed | dead')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { subscription?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.eventSubscriptionDelivery.findMany({ where: { ...(options.subscription ? { subscriptionId: options.subscription } : {}), ...(options.status ? { status: options.status as never } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500), include: { subscription: true, eventRecord: true } });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('delivery:retry')
    .requiredOption('--id <id>', 'delivery id')
    .option('--reset-attempts', 'обнулить счётчик попыток')
    .action(async (options: { id: string; resetAttempts?: boolean }) => withPrisma(async (prisma) => {
      const row = await prisma.eventSubscriptionDelivery.update({ where: { id: options.id }, data: { status: 'pending', availableAt: new Date(), lockedAt: null, lastError: null, ...(options.resetAttempts ? { attempts: 0 } : {}) } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('dead-letter:list')
    .option('--status <status>', 'open | resolved')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.eventDeadLetter.findMany({ where: options.status ? { status: options.status } : {}, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('dead-letter:resolve')
    .requiredOption('--id <id>', 'dead-letter id')
    .option('--actor <id>', 'actor id', 'cli')
    .action(async (options: { id: string; actor: string }) => withPrisma(async (prisma) => {
      const row = await prisma.eventDeadLetter.update({ where: { id: options.id }, data: { status: 'resolved', resolvedAt: new Date(), resolvedBy: options.actor } });
      console.log(JSON.stringify(row, null, 2));
    }));

  return command;
}

async function withPrisma<T>(fn: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    return await fn(prisma);
  } finally {
    await prisma.close();
  }
}

async function readJsonInput(payload?: string, payloadFile?: string): Promise<unknown> {
  if (payloadFile) return JSON.parse(await readFile(payloadFile, 'utf8'));
  if (payload) return JSON.parse(payload);
  return {};
}
