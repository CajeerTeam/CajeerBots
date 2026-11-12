import { createServer } from 'node:http';
import { loadConfigOrThrow } from '@cajeerbots/config';
import { PrismaService } from '@cajeerbots/db';
import { OutboxWorker } from './workers/outbox.worker.js';
import { ExtensionEventWorker } from './workers/extensions.worker.js';
import { BotOutboundWorker } from './workers/bot-outbound.worker.js';
import { EventApiDeliveryWorker } from './workers/event-api.worker.js';
import { SchedulerWorker } from './workers/scheduler.worker.js';
import { BackupRetentionWorker } from './workers/backup-retention.worker.js';
import { ObservabilityWorker } from './workers/observability.worker.js';
import { IntegrationTestsWorker } from './workers/integration-tests.worker.js';

async function main(): Promise<void> {
  loadConfigOrThrow();
  const prisma = new PrismaService();
  const workers = [new OutboxWorker(), new ExtensionEventWorker(), new BotOutboundWorker(), new EventApiDeliveryWorker(), new SchedulerWorker(), new BackupRetentionWorker(), new ObservabilityWorker(), new IntegrationTestsWorker()];
  let ready = false;
  let stopping = false;
  const server = createServer(async (req, res) => {
    if (req.method !== 'GET' || req.url !== '/worker/health') { res.writeHead(404).end(); return; }
    let ok = ready && !stopping;
    if (ok) { try { await prisma.health(); } catch { ok = false; } }
    res.writeHead(ok ? 200 : 503, { 'content-type': 'application/json' }).end(JSON.stringify({ ok, workers: workers.length }));
  });
  let startup: Promise<void> | undefined;
  let shutdown: Promise<void> | undefined;
  const stop = (): Promise<void> => {
    if (shutdown) return shutdown;
    stopping = true; ready = false;
    shutdown = (async () => {
      const deadline = setTimeout(() => process.exit(1), 30_000).unref();
      try {
        await startup?.catch(() => undefined);
        await Promise.allSettled(workers.map((worker) => worker.stop()));
        await new Promise<void>((resolve) => server.close(() => resolve()));
        await prisma.close();
      } finally { clearTimeout(deadline); }
    })();
    return shutdown;
  };
  const onSignal = () => { void stop().catch(() => { process.exitCode = 1; }); };
  process.once('SIGTERM', onSignal); process.once('SIGINT', onSignal);
  startup = (async () => {
    await prisma.connect();
    for (const worker of workers) {
      if (stopping) return;
      await worker.start();
    }
    if (stopping) return;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(Number(process.env.WORKER_HEALTH_PORT ?? 3102), process.env.WORKER_HEALTH_HOST ?? '0.0.0.0', () => { server.off('error', reject); resolve(); });
    });
    ready = !stopping;
  })();
  try { await startup; } catch (error) { await stop(); throw error; }
}
main().catch((error: unknown) => { console.error('[worker] startup failed:', error instanceof Error ? error.name : 'unknown'); process.exitCode = 1; });
