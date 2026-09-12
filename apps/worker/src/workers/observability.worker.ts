import { PrismaService } from '@cajeerbots/db';
import { collectObservabilitySnapshot, evaluateAlertRules, runDueProbes } from '@cajeerbots/observability';

export interface ObservabilityWorkerOptions {
  intervalMs?: number;
}

export class ObservabilityWorker {
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(private readonly prisma = new PrismaService(), private readonly options: ObservabilityWorkerOptions = {}) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    await this.tick();
    this.timer = setInterval(() => void this.tick(), this.options.intervalMs ?? Number(process.env.OBSERVABILITY_WORKER_INTERVAL_MS ?? 30000));
    console.log('[worker] observability worker 1.0.0-rc.1 started');
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
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
