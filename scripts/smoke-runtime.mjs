import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { startService, stopServices, waitHealth } from './runtime-processes.mjs';

// Use only an isolated, freshly migrated DB. Never run against production data.
if (process.env.CAJEER_SMOKE_ISOLATED !== '1') throw new Error('CAJEER_SMOKE_ISOLATED=1 required: use a disposable database');
const env = { ...process.env, NODE_ENV: 'test', APP_HOST: '127.0.0.1', APP_PORT: '3300', BOT_WEBHOOK_HOST: '127.0.0.1', BOT_WEBHOOK_PORT: '3301', WORKER_HEALTH_HOST: '127.0.0.1', WORKER_HEALTH_PORT: '3302', BOT_MODE: 'polling', TELEGRAM_BOT_TOKEN: '', VK_BOT_TOKEN: '', DISCORD_BOT_TOKEN: '', OTEL_ENABLED: 'false' };
const services = [];
try {
  const api = startService('api', env); services.push(api);
  const report = await waitHealth(api, 'http://127.0.0.1:3300/api/health');
  assert.equal(report.status, 'ok');
  const bot = startService('bot-runtime', env); services.push(bot);
  assert.equal((await waitHealth(bot, 'http://127.0.0.1:3301/runtime/health')).ok, true);
  const worker = startService('worker', env); services.push(worker);
  assert.equal((await waitHealth(worker, 'http://127.0.0.1:3302/worker/health')).ok, true);
  const cli = spawnSync(process.execPath, ['apps/cli/dist/main.js', '--version'], { env, encoding: 'utf8', timeout: 15_000 });
  assert.equal(cli.status, 0, cli.stderr);
  assert.equal(cli.stdout.trim(), '1.0.0-rc.3');
  const response = await fetch('http://127.0.0.1:3300/api/system/report', { headers: { 'x-api-key': env.CAJEER_ADMIN_TOKEN ?? '' }, signal: AbortSignal.timeout(5000) });
  assert.equal(response.status, 200); assert.equal((await response.json()).version, '1.0.0-rc.3');
  const rejected = await fetch('http://127.0.0.1:3300/api/communities', { signal: AbortSignal.timeout(5000) });
  assert.equal(rejected.status, 401);
  console.log('API, bot-runtime, worker and CLI smoke passed');
} finally { await stopServices(services); }
