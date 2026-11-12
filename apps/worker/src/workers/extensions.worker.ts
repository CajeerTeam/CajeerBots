import { SerialPoller } from '@cajeerbots/core';
import { PrismaService } from '@cajeerbots/db';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';

export interface ExtensionEventWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
}

export class ExtensionEventWorker {
  private poller?: SerialPoller;
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
    this.poller = new SerialPoller(this.options.pollIntervalMs ?? 5000, () => this.tick(), (error) => console.error('[worker] extensions.worker tick failed:', error instanceof Error ? error.name : 'unknown'));
    await this.poller.start();
    console.log('[worker] extension runtime worker 1.0.0-rc.3 started');
  }

  async stop(): Promise<void> {
    await this.poller?.stop();
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
