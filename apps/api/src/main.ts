import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import helmet from '@fastify/helmet';
import rateLimit from '@fastify/rate-limit';
import { Redis } from 'ioredis';
import { loadConfigOrThrow } from '@cajeerbots/config';
import { PrismaService } from '@cajeerbots/db';
import { startTracing } from '@cajeerbots/observability';
import { AppModule } from './modules/app.module.js';

async function bootstrap(): Promise<void> {
  const config = loadConfigOrThrow();
  await startTracing();
  const app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter({
    logger: { redact: ['req.headers.authorization', 'req.headers["x-api-key"]', 'req.headers.cookie'] },
  }), { rawBody: true });
  try {
    const prisma = app.get(PrismaService);
    await app.register(helmet);
    const redis = new Redis(config.REDIS_URL, { lazyConnect: true, connectTimeout: 5000, maxRetriesPerRequest: 1, enableOfflineQueue: false });
    redis.on('error', () => console.error('[api] Redis rate limiter unavailable'));
    app.getHttpAdapter().getInstance().addHook('onClose', async () => { redis.disconnect(); });
    await redis.connect();
    await app.register(rateLimit, {
      redis,
      skipOnError: false,
      nameSpace: 'cajeer:api:rate:', 
      max: config.API_RATE_LIMIT_MAX,
      timeWindow: config.API_RATE_LIMIT_WINDOW,
      onExceeding: async (request, key) => {
        try {
          await prisma.rateLimitViolation.create({ data: {
            bucket: 'api', identifier: key, route: request.routeOptions.url,
            limit: config.API_RATE_LIMIT_MAX, count: config.API_RATE_LIMIT_MAX + 1,
            windowMs: 60_000, ip: request.ip, userAgent: request.headers['user-agent'],
          } });
        } catch { /* Telemetry must not interrupt the request. */ }
      },
    });
    app.enableShutdownHooks();
    app.setGlobalPrefix('api');
    await app.listen({ port: config.APP_PORT, host: config.APP_HOST });
  } catch (error) {
    await app.close();
    throw error;
  }
}

bootstrap().catch((error: unknown) => {
  console.error('[api] startup failed:', error instanceof Error ? error.name : 'unknown');
  process.exitCode = 1;
});
