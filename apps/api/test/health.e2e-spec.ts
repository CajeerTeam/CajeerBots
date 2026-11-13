import 'reflect-metadata';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import request from 'supertest';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '@cajeerbots/db';
import { signJwt } from '@cajeerbots/security';
import { EventApiDeliveryWorker } from '../../worker/dist/workers/event-api.worker.js';
import { AppModule } from '../dist/modules/app.module.js';

let app: NestFastifyApplication;
beforeAll(async () => {
  app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter(), { logger: false, rawBody: true });
  app.setGlobalPrefix('api');
  await app.init();
  await app.getHttpAdapter().getInstance().ready();
});
afterAll(async () => { await app?.close(); });
describe('real API on migrated database', () => {
  it('reports healthy DB/cache/storage', async () => {
    const response = await request(app.getHttpServer()).get('/api/health').expect(200);
    expect(response.body.status).toBe('ok');
    expect(response.body.checks.database.status).toBe('ok');
  });
  it('does not allow unauthenticated community access', async () => {
    await request(app.getHttpServer()).get('/api/communities').expect(401);
  });
});


describe('RC3 security and durable delivery', () => {
  it('isolates tenant-scoped requests even with wildcard action permissions', async () => {
    const prisma = app.get(PrismaService);
    const a = await prisma.community.create({ data: { slug: `rc3-${randomUUID()}`, title: 'RC3 A' } });
    const b = await prisma.community.create({ data: { slug: `rc3-${randomUUID()}`, title: 'RC3 B' } });
    try {
      const { token } = signJwt({ sub: 'bootstrap-admin', typ: 'system', scopes: [`community:${a.id}`, '*'] }, process.env.JWT_SECRET!);
      const auth = `Bearer ${token}`;
      await request(app.getHttpServer()).get(`/api/communities/${a.id}`).set('Authorization', auth).expect(200);
      await request(app.getHttpServer()).get(`/api/communities/${b.id}`).set('Authorization', auth).expect(403);
      await request(app.getHttpServer()).get('/api/communities').set('Authorization', auth).expect(403);
    } finally { await prisma.community.deleteMany({ where: { id: { in: [a.id, b.id] } } }); }
  });
  it('claims once across workers, recovers stale leases, and preserves successful delivery', async () => {
    const prisma = app.get(PrismaService);
    const topic = await prisma.eventTopic.create({ data: { name: `rc3.${randomUUID()}` } });
    const record = await prisma.eventRecord.create({ data: { eventId: randomUUID(), topicId: topic.id, topicName: topic.name, type: topic.name, payload: {} } });
    const subscription = await prisma.eventSubscription.create({ data: { name: `rc3-${randomUUID()}`, topicId: topic.id, topicPattern: topic.name, targetType: 'log', targetRef: 'test' } });
    try {
      const delivery = await prisma.eventSubscriptionDelivery.create({ data: { eventRecordId: record.id, subscriptionId: subscription.id } });
      const a = new EventApiDeliveryWorker(prisma), b = new EventApiDeliveryWorker(prisma);
      await Promise.all([a.deliver(delivery.id), b.deliver(delivery.id)]);
      let result = await prisma.eventSubscriptionDelivery.findUniqueOrThrow({ where: { id: delivery.id } });
      expect(result.status).toBe('delivered'); expect(result.attempts).toBe(1);
      await a.deliver(delivery.id);
      expect((await prisma.eventSubscriptionDelivery.findUniqueOrThrow({ where: { id: delivery.id } })).attempts).toBe(1);
      await prisma.eventSubscriptionDelivery.update({ where: { id: delivery.id }, data: { status: 'processing', lockedAt: new Date(Date.now() - 600_000) } });
      await Promise.all([a.deliver(delivery.id), b.deliver(delivery.id)]);
      result = await prisma.eventSubscriptionDelivery.findUniqueOrThrow({ where: { id: delivery.id } });
      expect(result.status).toBe('delivered'); expect(result.attempts).toBe(2);
    } finally {
      await prisma.eventRecord.delete({ where: { id: record.id } });
      await prisma.eventSubscription.delete({ where: { id: subscription.id } });
      await prisma.eventTopic.delete({ where: { id: topic.id } });
    }
  });
});

describe('community-scoped event publication', () => {
  it('creates one durable delivery for the matching community and none for global or other-community subscriptions', async () => {
    const prisma = app.get(PrismaService);
    const token = process.env.CAJEER_ADMIN_TOKEN;
    if (!token) throw new Error('CAJEER_ADMIN_TOKEN required for event API E2E');
    const id = randomUUID();
    const topicName = `rehearsal.${id}`;
    const a = await prisma.community.create({ data: { slug: `event-a-${id}`, title: 'Event A' } });
    const b = await prisma.community.create({ data: { slug: `event-b-${id}`, title: 'Event B' } });
    const topic = await prisma.eventTopic.create({ data: { name: topicName } });
    try {
      const create = (name: string, filters?: { communityId: string }) =>
        request(app.getHttpServer()).post('/api/events/subscriptions').set('x-api-key', token)
          .send({ name: `${name}-${id}`, topicPattern: topicName, targetType: 'log', targetRef: 'rehearsal', ...(filters ? { filters } : {}) }).expect(201);
      const scoped = await create('scoped', { communityId: a.id });
      const global = await create('global');
      const other = await create('other', { communityId: b.id });
      expect(scoped.body.filters).toEqual({ communityId: a.id });
      const event = { id, type: topicName, communityId: a.id, payload: { index: 0 } };
      const publish = () => request(app.getHttpServer()).post('/api/events/publish').set('x-api-key', token).send(event).expect(201);
      const first = await publish();
      const duplicate = await publish();
      expect(first.body.accepted).toBe(true);
      expect(first.body.matchedSubscriptions).toBe(1);
      expect(duplicate.body.event.id).toBe(first.body.event.id);
      const rows = await prisma.eventSubscriptionDelivery.findMany({ where: { eventRecordId: first.body.event.id } });
      expect(rows).toHaveLength(1);
      expect(rows[0]?.subscriptionId).toBe(scoped.body.id);
      expect(rows[0]?.status).toBe('pending');
      expect(rows[0]?.attempts).toBe(0);
      expect(await prisma.eventSubscriptionDelivery.count({ where: { subscriptionId: { in: [global.body.id, other.body.id] } } })).toBe(0);
      const delivery = rows[0];
      if (!delivery) throw new Error('Expected one durable delivery');
      const worker = new EventApiDeliveryWorker(prisma);
      await worker.deliver(delivery.id);
      const delivered = await prisma.eventSubscriptionDelivery.findUniqueOrThrow({ where: { id: delivery.id } });
      expect(delivered.status).toBe('delivered');
      expect(delivered.attempts).toBe(1);
    } finally {
      await prisma.eventRecord.deleteMany({ where: { topicId: topic.id } });
      await prisma.eventSubscription.deleteMany({ where: { topicId: topic.id } });
      await prisma.eventTopic.delete({ where: { id: topic.id } });
      await prisma.community.deleteMany({ where: { id: { in: [a.id, b.id] } } });
    }
  });
});
