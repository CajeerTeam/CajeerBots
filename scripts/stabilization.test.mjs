import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { SerialPoller } from '../packages/core/dist/runtime-lifecycle.js';

test('SerialPoller waits for active tick and does not schedule after stop', async () => {
  let count = 0, active = 0, maximum = 0;
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const loop = new SerialPoller(5, async () => { count++; active++; maximum = Math.max(maximum, active); await gate; active--; }, () => assert.fail('unexpected tick failure'));
  const starting = loop.start();
  await delay(10);
  const stopping = loop.stop();
  await delay(10);
  assert.equal(active, 1);
  release(); await starting; await stopping;
  await delay(20);
  assert.equal(count, 1); assert.equal(maximum, 1); assert.equal(active, 0);
});
test('SerialPoller recovers from periodic errors', async () => {
  let count = 0, errors = 0;
  let completed;
  const done = new Promise(resolve => { completed = resolve; });
  const loop = new SerialPoller(5, async () => { count++; if (count === 2) throw new Error('temporary'); if (count === 3) completed(); }, () => { errors++; });
  await loop.start();
  try { await Promise.race([done, delay(500).then(() => { if (count < 3) throw new Error('no retry'); })]); } finally { await loop.stop(); }
  assert.equal(errors, 1); assert.ok(count >= 3);
});
test('first tick failure is reported and does not start polling', async () => {
  let count = 0;
  const loop = new SerialPoller(5, async () => { count++; throw new Error('DB unavailable'); }, () => {});
  await assert.rejects(loop.start(), /DB unavailable/);
  await delay(20); assert.equal(count, 1);
});

import { generateProductionEnv, parseEnvText, validateProductionEnv } from '../packages/deployment/dist/env.js';
import { mkdtemp, copyFile, mkdir, readFile, stat, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

test('production env generator matches actual runtime names and credentials', () => {
  const env = parseEnvText(generateProductionEnv({ appUrl: 'https://bots.example.org' }));
  assert.equal(validateProductionEnv(env).valid, true);
  assert.equal(new URL(env.DATABASE_URL).password, env.POSTGRES_PASSWORD);
  assert.equal(env.BOT_WEBHOOK_PORT, '3101');
  assert.equal(env.BOT_MODE, 'polling');
  for (const key of ['JWT_SECRET', 'CAJEER_ADMIN_TOKEN', 'SECRETS_ENCRYPTION_KEY', 'API_TOKEN_PEPPER', 'WEBHOOK_SIGNING_SECRET', 'PASSWORD_PEPPER', 'REFRESH_TOKEN_PEPPER', 'RECOVERY_CODE_PEPPER']) assert.ok(env[key].length >= 32);
});
test('production setup protects secrets and refuses to overwrite an existing environment', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'cajeer-env-'));
  try {
    await mkdir(join(dir, 'scripts'));
    await copyFile(new URL('./setup-production.mjs', import.meta.url), join(dir, 'scripts/setup-production.mjs'));
    await copyFile(new URL('../.env.production.example', import.meta.url), join(dir, '.env.production.example'));
    const run = () => spawnSync(process.execPath, [join(dir, 'scripts/setup-production.mjs'), 'https://test.example.org'], { encoding: 'utf8' });
    assert.equal(run().status, 0);
    const file = join(dir, '.env.production'); const first = await readFile(file, 'utf8');
    assert.equal((await stat(file)).mode & 0o777, 0o600);
    assert.equal(parseEnvText(first).APP_PUBLIC_URL, 'https://test.example.org');
    assert.notEqual(parseEnvText(first).JWT_SECRET, parseEnvText(first).API_TOKEN_PEPPER);
    assert.notEqual(run().status, 0); assert.equal(await readFile(file, 'utf8'), first);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
