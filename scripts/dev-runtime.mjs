import { startService, stopServices } from './runtime-processes.mjs';
const services = ['api', 'bot-runtime', 'worker'].map(name => startService(name, process.env, true));
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  try { await stopServices(services); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
process.once('SIGINT', () => void stop());
process.once('SIGTERM', () => void stop());
await Promise.race(services.map(s => s.exited));
if (!stopping) { process.exitCode = 1; await stop(); }
