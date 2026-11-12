import { access, mkdir, writeFile, rm } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import type { HealthStatus } from './health.js';

export interface DiagnosticCheck {
  status: HealthStatus;
  message: string;
  latencyMs?: number;
}

export interface DiagnosticReport {
  status: HealthStatus;
  checks: Record<'database' | 'cache' | 'queue' | 'storage', DiagnosticCheck>;
}

export interface DiagnosticOptions {
  databaseUrl?: string;
  redisUrl?: string;
  queueDriver?: 'redis' | 'postgres' | 'rabbitmq' | string;
  storageRoot?: string;
  databaseProbe?: () => Promise<void>;
}

export async function createDiagnosticReport(options: DiagnosticOptions = {}): Promise<DiagnosticReport> {
  const [database, cache, storage] = await Promise.all([checkDatabase(options), checkRedis(options.redisUrl), checkStorage(options.storageRoot ?? './storage')]);
  const driver = options.queueDriver ?? 'redis';
  const queue: DiagnosticCheck = driver === 'redis' ? cache : driver === 'postgres' ? database : { status: 'degraded', message: 'External queue probe unavailable' };
  const checks = { database, cache, queue, storage };
  const statuses = Object.values(checks).map((check) => check.status);
  const status: HealthStatus = statuses.includes('down') ? 'down' : statuses.includes('degraded') ? 'degraded' : 'ok';
  return { status, checks };
}

async function checkDatabase(options: DiagnosticOptions): Promise<DiagnosticCheck> {
  if (options.databaseProbe) return timed('PostgreSQL доступен', options.databaseProbe);
  return checkTcpUrl(options.databaseUrl, 'PostgreSQL');
}

async function checkStorage(root: string): Promise<DiagnosticCheck> {
  const probe = join(root, `.cajeer-healthcheck-${randomUUID()}`);
  return timed(`Local storage доступен: ${root}`, async () => {
    await mkdir(dirname(probe), { recursive: true });
    try { await writeFile(probe, 'ok', { flag: 'wx' }); await access(probe); } finally { await rm(probe, { force: true }); }
  });
}

async function checkRedis(rawUrl: string | undefined): Promise<DiagnosticCheck> {
  if (!rawUrl) return { status: 'down', message: 'Redis: URL не задан' };
  return timed('Redis PING', async () => {
    const redis = new Redis(rawUrl, { lazyConnect: true, connectTimeout: 1500, commandTimeout: 1500, maxRetriesPerRequest: 0, retryStrategy: () => null, enableOfflineQueue: false });
    redis.on('error', () => undefined);
    try {
      await redis.connect();
      if (await redis.ping() !== 'PONG') throw new Error('unexpected Redis response');
    } catch { throw new Error('Redis PING failed'); }
    finally { redis.disconnect(); }
  });
}

async function checkTcpUrl(rawUrl: string | undefined, label: string): Promise<DiagnosticCheck> {
  if (!rawUrl) return { status: 'down', message: `${label}: URL не задан` };
  try {
    const url = new URL(rawUrl);
    const port = Number(url.port || (url.protocol === 'rediss:' || url.protocol === 'https:' ? 443 : url.protocol === 'postgresql:' ? 5432 : 6379));
    return timed(`${label} доступен: ${url.hostname}:${port}`, () => tcpConnect(url.hostname, port));
  } catch {
    return { status: 'down', message: `${label}: invalid configuration` };
  }
}

async function timed(message: string, fn: () => Promise<void>): Promise<DiagnosticCheck> {
  const started = Date.now();
  try {
    await fn();
    return { status: 'ok', message, latencyMs: Date.now() - started };
  } catch {
    return { status: 'down', message: `${message}: probe failed`, latencyMs: Date.now() - started };
  }
}

function tcpConnect(host: string, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host, port, timeout: 1500 });
    socket.once('connect', () => { socket.destroy(); resolve(); });
    socket.once('timeout', () => { socket.destroy(); reject(new Error('timeout')); });
    socket.once('error', reject);
  });
}
