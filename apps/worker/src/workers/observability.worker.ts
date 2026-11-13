import { SerialPoller } from '@cajeerbots/core';
import { PrismaService } from '@cajeerbots/db';
import { collectObservabilitySnapshot, evaluateAlertRules, runDueProbes } from '@cajeerbots/observability';

export interface ObservabilityWorkerOptions {
  intervalMs?: number;
}

export class ObservabilityWorker {
  private poller?: SerialPoller;
  private running = false;

  constructor(private readonly prisma = new PrismaService(), private readonly options: ObservabilityWorkerOptions = {}) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    this.poller = new SerialPoller(this.options.intervalMs ?? Number(process.env.OBSERVABILITY_WORKER_INTERVAL_MS ?? 30000), () => this.tick(), (error) => console.error('[worker] observability.worker tick failed:', error instanceof Error ? error.name : 'unknown'));
    await this.poller.start();
    console.log('[worker] observability worker 1.0.1 started');
  }

  async stop(): Promise<void> {
    await this.poller?.stop();
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const snapshot = await collectObservabilitySnapshot(this.prisma, { includeDiagnostics: true });
      const probes = await runDueProbes(this.prisma);
      const alerts = await evaluateAlertRules(this.prisma);
      if (snapshot.inserted > 0 || probes.checked > 0 || alerts.fired > 0 || alerts.resolved > 0) {
        console.log(`[worker] observability collected=${snapshot.inserted} probes=${probes.checked} fired=${alerts.fired} resolved=${alerts.resolved}`);
      }
    } catch (error) {
      console.error('[worker] observability failed', error);
    } finally {
      this.running = false;
    }
  }
}
