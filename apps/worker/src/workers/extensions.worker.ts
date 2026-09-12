import { PrismaService } from '@cajeerbots/db';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';

export interface ExtensionEventWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
}

export class ExtensionEventWorker {
  private timer?: NodeJS.Timeout;
  private running = false;
  private readonly runtime: DbExtensionRuntime;

  constructor(
    private readonly prisma = new PrismaService(),
    private readonly options: ExtensionEventWorkerOptions = {},
  ) {
    this.runtime = new DbExtensionRuntime(prisma, { maxAttempts: options.maxAttempts });
  }

  async start(): Promise<void> {
    await this.prisma.connect();
    await this.tick();
    this.timer = setInterval(() => void this.tick(), this.options.pollIntervalMs ?? 5000);
    console.log('[worker] extension runtime worker 1.0.0-rc.1 started');
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      await this.runtime.processPending(this.options.batchSize ?? 25);
    } finally {
      this.running = false;
    }
  }
}
