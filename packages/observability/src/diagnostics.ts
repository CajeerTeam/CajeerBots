import { access, mkdir, writeFile, rm } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { dirname, join } from 'node:path';
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
  const checks = {
    database: await checkDatabase(options),
    cache: await checkTcpUrl(options.redisUrl, 'Redis'),
    queue: await checkQueue(options),
    storage: await checkStorage(options.storageRoot ?? './storage'),
  };
  const statuses = Object.values(checks).map((check) => check.status);
  const status: HealthStatus = statuses.includes('down') ? 'down' : statuses.includes('degraded') ? 'degraded' : 'ok';
  return { status, checks };
}

async function checkDatabase(options: DiagnosticOptions): Promise<DiagnosticCheck> {
  if (options.databaseProbe) return timed('PostgreSQL доступен', options.databaseProbe);
  return checkTcpUrl(options.databaseUrl, 'PostgreSQL');
}

async function checkQueue(options: DiagnosticOptions): Promise<DiagnosticCheck> {
  const driver = options.queueDriver ?? 'redis';
  if (driver === 'redis') return checkTcpUrl(options.redisUrl, 'Redis Queue');
  if (driver === 'postgres') return checkDatabase(options);
  return { status: 'degraded', message: `Драйвер очереди ${driver} требует отдельного runtime-плагина` };
}

async function checkStorage(root: string): Promise<DiagnosticCheck> {
  const probe = join(root, '.cajeer-healthcheck');
  return timed(`Local storage доступен: ${root}`, async () => {
    await mkdir(dirname(probe), { recursive: true });
    await writeFile(probe, 'ok');
    await access(probe);
    await rm(probe, { force: true });
  });
}

async function checkTcpUrl(rawUrl: string | undefined, label: string): Promise<DiagnosticCheck> {
  if (!rawUrl) return { status: 'down', message: `${label}: URL не задан` };
  try {
    const url = new URL(rawUrl);
    const port = Number(url.port || (url.protocol === 'rediss:' || url.protocol === 'https:' ? 443 : url.protocol === 'postgresql:' ? 5432 : 6379));
    return timed(`${label} доступен: ${url.hostname}:${port}`, () => tcpConnect(url.hostname, port));
  } catch (error) {
    return { status: 'down', message: `${label}: ${(error as Error).message}` };
  }
}

async function timed(message: string, fn: () => Promise<void>): Promise<DiagnosticCheck> {
  const started = Date.now();
  try {
    await fn();
    return { status: 'ok', message, latencyMs: Date.now() - started };
  } catch (error) {
    return { status: 'down', message: `${message}: ${(error as Error).message}`, latencyMs: Date.now() - started };
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
