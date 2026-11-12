/** One tick at a time; stop waits for an in-flight task before releasing resources. */
export class SerialPoller {
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active: Promise<void> | undefined;
  private stopped = true;
  constructor(private readonly intervalMs: number, private readonly task: () => Promise<void>, private readonly onError: (error: unknown) => void) {
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) throw new Error('Некорректный интервал worker');
  }
  async start(): Promise<void> {
    if (!this.stopped) return;
    this.stopped = false;
    try { await this.run(); } catch (error) { this.stopped = true; throw error; }
    this.schedule();
  }
  private run(): Promise<void> {
    const task = Promise.resolve().then(this.task);
    this.active = task;
    return task.finally(() => { if (this.active === task) this.active = undefined; });
  }
  private schedule(): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.run().catch(this.onError).finally(() => this.schedule());
    }, this.intervalMs);
  }
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    await this.active?.catch(() => undefined);
  }
}
