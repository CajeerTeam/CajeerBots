import 'reflect-metadata';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import request from 'supertest';
import { AppModule } from '../dist/modules/app.module.js';

let app: NestFastifyApplication;
beforeAll(async () => {
  app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter(), { logger: false });
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
