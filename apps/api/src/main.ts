import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import helmet from '@fastify/helmet';
import rateLimit from '@fastify/rate-limit';
import { loadConfigOrThrow } from '@cajeerbots/config';
import { startTracing } from '@cajeerbots/observability';
import { AppModule } from './modules/app.module.js';
import { PrismaClient } from '../../../prisma/generated/client/index.js';

async function bootstrap(): Promise<void> {
  const config = loadConfigOrThrow();
  await startTracing();

  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({ logger: true }),
  );

  await app.register(helmet);
  const rateLimitPrisma = new PrismaClient();
  await app.register(rateLimit, {
    max: Number(process.env.API_RATE_LIMIT_MAX ?? 300),
    timeWindow: process.env.API_RATE_LIMIT_WINDOW ?? '1 minute',
    onExceeding: async (request: any, key: string) => {
      try {
        await rateLimitPrisma.rateLimitViolation.create({
          data: {
            bucket: 'api',
            identifier: key,
            route: request.routeOptions?.url,
            limit: Number(process.env.API_RATE_LIMIT_MAX ?? 300),
            count: Number(request.rateLimit?.current ?? 0),
            windowMs: 60_000,
            ip: request.ip,
            userAgent: request.headers?.['user-agent'],
          },
        });
      } catch {
        // Rate-limit telemetry must never break user requests.
      }
    },
  } as any);

  app.enableShutdownHooks();
  app.setGlobalPrefix('api');

  await app.listen({ port: config.APP_PORT, host: config.APP_HOST });
}

void bootstrap();
