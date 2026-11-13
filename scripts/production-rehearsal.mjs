#!/usr/bin/env node
import assert from 'node:assert/strict';
import { randomBytes, randomUUID, generateKeyPairSync } from 'node:crypto';
import { mkdtempSync, readFileSync, writeFileSync, rmSync, mkdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const [baselineImage, targetImage, reportArg, countArg = '100', concurrencyArg = '8'] = process.argv.slice(2);
assert(baselineImage && targetImage && reportArg, 'Usage: production-rehearsal.mjs BASELINE_IMAGE TARGET_IMAGE REPORT [COUNT] [CONCURRENCY]');
assert.notEqual(baselineImage, targetImage);
assert.equal(process.env.CAJEER_REHEARSAL_ISOLATED, '1', 'Explicit disposable instance confirmation required');
const count = Number(countArg), concurrency = Number(concurrencyArg);
assert(Number.isInteger(count) && count >= 1 && count <= 10000 && Number.isInteger(concurrency) && concurrency >= 1 && concurrency <= 64);
const reportPath = resolve(reportArg); mkdirSync(dirname(reportPath), { recursive: true });
const dir = mkdtempSync(join(tmpdir(), 'cajeer-rehearsal-'));
const source = `cajeer-stable-${randomUUID().slice(0, 8)}`, restored = `${source}-restore`, clean = `${source}-clean`, stableRestored = `${source}-stable-restore`;
const envFile = join(dir, 'env.production'), backup = join(dir, 'backup');
const password = randomBytes(32).toString('base64url');
const { privateKey, publicKey } = generateKeyPairSync('ed25519');
const publicHex = publicKey.export({ type: 'spki', format: 'der' }).subarray(-32).toString('hex');
const fixtureConfig = `\nBOT_WEBHOOK_CREDENTIALS={"rehearsal-discord":{"publicKey":"${publicHex}"}}\nCAJEER_REHEARSAL_PRIVATE_KEY=${privateKey.export({ type: 'pkcs8', format: 'der' }).toString('base64')}\nDISCORD_BOT_TOKEN_REHEARSAL=fixture-no-provider-network\n`; 
writeFileSync(envFile, readFileSync(join(root, '.env.production.example'), 'utf8')
  .replace('BOT_WEBHOOK_CREDENTIALS={}', '')
  .replaceAll('replace-with-generated-password', password).replaceAll('replace-with-generated-secret', () => randomBytes(48).toString('base64url')) + '\nCAJEER_REHEARSAL_ISOLATED=1\nCAJEER_API_HOST_PORT=0\nCAJEER_BOT_HOST_PORT=0\nAPI_RATE_LIMIT_MAX=50000\nEVENT_API_WORKER_INTERVAL_MS=250\nEVENT_API_WORKER_BATCH_SIZE=100\n' + fixtureConfig, { mode: 0o600 });
const report = { format: 'cajeerbots.rehearsal.v1', startedAt: new Date().toISOString(), baselineImage, targetImage, baselineVersion: '1.0.0-rc.6', targetVersion: '1.0.0', count, concurrency, phases: [], passed: false };
const save = () => writeFileSync(reportPath, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
function run(cmd, args, options = {}) {
  const result = spawnSync(cmd, args, { cwd: root, env: options.env ?? process.env, timeout: 30 * 60 * 1000, encoding: 'utf8', maxBuffer: 16 * 1024 * 1024, stdio: ['ignore', options.capture ? 'pipe' : 'inherit', 'inherit'] });
  if (result.error || result.status !== 0) throw new Error(`${cmd} ${args.slice(0, 2).join(' ')} failed (${result.error?.message ?? result.status ?? result.signal})`);
  return result.stdout?.trim();
}
function compose(project, image, args, capture = false) {
  return run('docker', ['compose', '--project-name', project, '--env-file', envFile, '-f', 'docker-compose.prod.yml', ...args], { capture, env: { ...process.env, CAJEER_ENV_FILE: envFile, CAJEER_IMAGE: image, COMPOSE_PROFILES: '' } });
}
function ops(...args) { return run(process.execPath, ['scripts/instance-ops.mjs', ...args]); }
function phase(name, fn) {
  const start = Date.now();
  try { const result = fn(); if (result?.load?.passed === false) { report.phases.push({ name, passed: false, durationMs: Date.now() - start, result }); save(); throw Error('HTTP load gate failed'); } report.phases.push({ name, passed: true, durationMs: Date.now() - start, ...(result === undefined ? {} : { result }) }); save(); return result; }
  catch (e) { if (report.phases.at(-1)?.name !== name) report.phases.push({ name, passed: false, durationMs: Date.now() - start, error: e.message }); save(); throw e; }
}
function installWorkload(project, image) {
  for (const file of ['rehearsal-workload.mjs', 'rehearsal-load.mjs']) compose(project, image, ['cp', `scripts/${file}`, `api:/app/scripts/${file}`]);
}
function workload(project, image, action, state, ...args) {
  const encoded = state ? Buffer.from(JSON.stringify(state)).toString('base64url') : '';
  return JSON.parse(compose(project, image, ['exec', '-T', 'api', 'node', 'scripts/rehearsal-workload.mjs', action, encoded, ...args.map(String)], true));
}
let state;
try {
  report.commit = run('git', ['rev-parse', 'HEAD'], { capture: true });
  report.baselineImageId = run('docker', ['image', 'inspect', '--format', '{{.Id}}', baselineImage], { capture: true });
  report.targetImageId = run('docker', ['image', 'inspect', '--format', '{{.Id}}', targetImage], { capture: true });
  assert.equal(run('docker',['image','inspect','--format','{{index .Config.Labels "org.opencontainers.image.revision"}}',targetImage],{capture:true}), report.commit, 'Target image must be built from current commit');
  phase('clean-install-stable', () => {
    compose(clean, targetImage, ['up', '-d', '--no-build', '--wait', '--wait-timeout', '240']);
    assert.equal(compose(clean, targetImage, ['exec', '-T', 'api', 'node', 'apps/cli/dist/main.js', '--version'], true), '1.0.0');
  });
  phase('pilot-capacity-enforcement', () => {
    compose(clean,targetImage,['exec','-T','api','node','scripts/pilot-capacity-e2e.mjs']);
    const status=JSON.parse(compose(clean,targetImage,['exec','-T','api','node','scripts/pilot-probe.mjs'],true));
    assert.equal(status.passed,true);
    compose(clean,targetImage,['down','-v','--remove-orphans']);
    return status;
  });
  phase('clean-install-rc6', () => {
    compose(source, baselineImage, ['up', '-d', '--no-build', '--wait', '--wait-timeout', '240']);
    assert.equal(compose(source, baselineImage, ['exec', '-T', 'api', 'node', 'apps/cli/dist/main.js', '--version'], true), '1.0.0-rc.6');
    installWorkload(source, baselineImage);
  });
  state = phase('rc6-community-backup-and-sentinels', () => workload(source, baselineImage, 'prepare'));
  phase('worker-unavailable', () => compose(source, baselineImage, ['stop', '-t', '40', 'worker']));
  // Test backlog persistence during a worker outage.
  state = phase('http-load-durable-backlog', () => workload(source, baselineImage, 'publish', state, count, concurrency, process.env.REHEARSAL_P95_MS ?? 1500, process.env.REHEARSAL_MIN_RPS ?? 0));
  // Restart worker and drain so instance backup has a consistent delivered-state baseline.
  phase('worker-restart', () => compose(source, baselineImage, ['start', 'worker']));
  state = phase('queue-drain', () => workload(source, baselineImage, 'drain', state, process.env.REHEARSAL_DRAIN_MS ?? 180000));
  phase('upgrade-rc6-to-stable', () => ops('upgrade', source, envFile, baselineImage, backup, targetImage, '1.0.0'));
  phase('verify-upgraded-data', () => { installWorkload(source, targetImage); return workload(source, targetImage, 'verify', state); });
  // New RC6 work exercises duplicate IDs under load after migrating the old data.
  let after = phase('stable-community-backup-restore', () => workload(source, targetImage, 'prepare'));
  phase('stable-stop-worker', () => compose(source, targetImage, ['stop', '-t', '40', 'worker']));
  phase('create-discord-fixture', () => workload(source, targetImage, 'botSetup', after));
  phase('reload-bot-runtime', () => compose(source, targetImage, ['restart', 'bot-runtime']));
  phase('signed-discord-form-e2e', () => workload(source, targetImage, 'botTest', after));
  after = phase('stable-http-load', () => workload(source, targetImage, 'publish', after, count, concurrency, process.env.REHEARSAL_P95_MS ?? 1500, process.env.REHEARSAL_MIN_RPS ?? 0));
  phase('stable-restart-worker', () => compose(source, targetImage, ['start', 'worker']));
  phase('stable-queue-drain', () => workload(source, targetImage, 'drain', after, process.env.REHEARSAL_DRAIN_MS ?? 180000));
  phase('restore-original-into-fresh-project', () => ops('restore', restored, backup));
  phase('start-restored-rc6', () => ops('start', restored, join(backup, 'env.production'), baselineImage, '1.0.0-rc.6'));
  phase('verify-restored-data', () => { installWorkload(restored, baselineImage); return workload(restored, baselineImage, 'verify', state); });
  phase('stable-instance-backup-restore', () => {
    const stableBackup = join(dir, 'stable-backup');
    ops('backup', source, envFile, targetImage, stableBackup);
    ops('restore', stableRestored, stableBackup);
    ops('start', stableRestored, join(stableBackup, 'env.production'), targetImage, '1.0.0');
    return workload(stableRestored, targetImage, 'verify', after);
  });
  report.passed = true;
} catch (error) { console.error(error.message); process.exitCode = 1; }
finally {
  for (const [project, image] of [[source, targetImage], [restored, baselineImage], [clean, targetImage], [stableRestored, targetImage]]) {
    try { compose(project, image, ['down', '-v', '--remove-orphans']); }
    catch (error) { report.passed = false; report.phases.push({ name: `cleanup-${project}`, passed: false, error: error.message }); process.exitCode = 1; }
  }
  rmSync(dir, { recursive: true, force: true });
  report.finishedAt = new Date().toISOString(); save();
  console.log(`Rehearsal report: ${reportPath}`);
}
