import { loadConfigOrThrow } from '@cajeerbots/config';
import { createRuntime } from './runtime/create-runtime.js';

async function main(): Promise<void> {
  const config = loadConfigOrThrow();
  const runtime = await createRuntime({ mode: config.BOT_MODE });
  let shutdown: Promise<void> | undefined;
  const stop = (): Promise<void> => {
    if (shutdown) return shutdown;
    shutdown = (async () => {
      const timeout = setTimeout(() => process.exit(1), 30_000).unref();
      try { await startup.catch(() => undefined); await runtime.stop(); } finally { clearTimeout(timeout); }
    })();
    return shutdown;
  };
  const onSignal = () => { void stop().catch(() => { process.exitCode = 1; }); };
  process.once('SIGTERM', onSignal); process.once('SIGINT', onSignal);
  const startup = runtime.start();
  try { await startup; } catch (error) { await stop(); throw error; }
}
main().catch((error: unknown) => { console.error('[bot-runtime] startup failed:', error instanceof Error ? error.name : 'unknown'); process.exitCode = 1; });
