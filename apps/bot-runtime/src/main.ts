import { createRuntime } from './runtime/create-runtime.js';

async function main(): Promise<void> {
  const runtime = await createRuntime({ mode: process.env.BOT_MODE === 'webhook' ? 'webhook' : 'polling' });
  await runtime.start();

  const stop = async () => {
    await runtime.stop();
    process.exit(0);
  };
  process.once('SIGTERM', () => void stop());
  process.once('SIGINT', () => void stop());
}

void main();
