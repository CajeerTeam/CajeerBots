// Runs inside API container against actual HTTP, PostgreSQL, Redis and mounted storage.
import assert from 'node:assert/strict';
import { randomUUID, createPrivateKey, sign } from 'node:crypto';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { PrismaClient } from '../prisma/generated/client/index.js';
import { Redis } from 'ioredis';
import { spawnSync } from 'node:child_process';
import { runLoad } from './rehearsal-load.mjs';

assert.equal(process.env.CAJEER_REHEARSAL_ISOLATED, '1', 'Only disposable rehearsal instances');
const db = new PrismaClient();
const redis = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 1, connectTimeout: 5000 });
const api = 'http://127.0.0.1:3000/api';
const token = process.env.CAJEER_ADMIN_TOKEN;
assert(token, 'Admin token required');
const [action, encoded, countArg, concurrencyArg, p95Arg, rpsArg] = process.argv.slice(2);
const state = encoded ? JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8')) : null;
async function request(method, path, body, expected, authenticated = true) {
  const response = await fetch(api + path, { method, headers: { ...(authenticated ? { 'x-api-key': token } : {}), ...(body === undefined ? {} : { 'content-type': 'application/json' }) }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10000) });
  const text = await response.text();
  assert((expected ?? [200, 201]).includes(response.status), `${method} ${path} returned ${response.status}`);
  return JSON.parse(text);
}
const sentinelPath = (root, id) => join(root, 'rehearsal', `${id}.txt`);
async function prepare() {
  const id = randomUUID(); const title = `Rehearsal ${id}`;
  await request('GET', '/communities', undefined, [401], false);
  const community = await request('POST', '/communities', { slug: `rehearsal-${id}`, title });
  const topic = `rehearsal.${id}`;
  await request('PUT', `/events/topics/${topic}`, { description: 'Production rehearsal' });
  const sub = await request('POST', '/events/subscriptions', { name: `rehearsal-${id}`, topicPattern: topic, targetType: 'log', targetRef: 'rehearsal' });
  // Community backup is a real local-storage object; change data, dry-run, then restore.
  const backup = await request('POST', `/communities/${community.id}/backups`, { title });
  assert.equal(backup.status, 'created');
  assert.equal((await request('POST', `/backups/${backup.id}/verify`, {})).valid, true);
  const version = (await request('GET', '/system/report')).version;
  // RC4 is the historical baseline; exercise the hardened merge path on RC5.
  if (['1.0.0-rc.5', '1.0.0-rc.6', '1.0.0'].includes(version)) {
  await request('PATCH', `/communities/${community.id}`, { title: 'Changed before restore' });
  assert.equal((await request('POST', `/backups/${backup.id}/restore`, { apply: false })).status, 'completed');
  assert.equal((await request('GET', `/communities/${community.id}`)).title, 'Changed before restore');
  assert.equal((await request('POST', `/backups/${backup.id}/restore`, { apply: true })).status, 'completed');
  assert.equal((await request('GET', `/communities/${community.id}`)).title, title);
  // A valid payload wrapped in modified bytes must fail object-level integrity.
  const path = join(process.env.STORAGE_LOCAL_ROOT, backup.storageKey);
  const original = await readFile(path);
  try {
    const modified = JSON.parse(original.toString('utf8')); modified.title = 'tampered';
    await writeFile(path, JSON.stringify(modified));
    assert.equal((await request('POST', `/backups/${backup.id}/verify`, {})).valid, false);
    assert.equal((await request('POST', `/backups/${backup.id}/restore`, { apply: true })).status, 'failed');
    const cli = spawnSync(process.execPath, ['apps/cli/dist/main.js', 'backup', 'restore', backup.id, '--community', community.id, '--apply'], { encoding: 'utf8', timeout: 30000 });
    assert.equal(cli.error, undefined); assert.notEqual(cli.status, 0);
    assert.equal((await request('GET', `/communities/${community.id}`)).title, title);
  } finally { await writeFile(path, original); }
  assert.equal((await request('POST', `/backups/${backup.id}/verify`, {})).valid, true);
  }
  const redisKey = `rehearsal:${id}`;
  await redis.set(redisKey, id);
  for (const root of [process.env.STORAGE_LOCAL_ROOT, process.env.EXTENSIONS_ROOT]) {
    await mkdir(join(root, 'rehearsal'), { recursive: true });
    await writeFile(sentinelPath(root, id), id, { flag: 'wx' });
  }
  return { id, title, communityId: community.id, topic, subscriptionId: sub.id, backupId: backup.id, redisKey };
}
function event(i) { return { id: `${state.id}-${i}`, type: state.topic, communityId: state.communityId, payload: { index: i } }; }
async function publish() {
  const count = Number(countArg ?? 100); const concurrency = Number(concurrencyArg ?? 8);
  const system = await request('GET', '/system/report');
  if (['1.0.0-rc.5', '1.0.0-rc.6', '1.0.0'].includes(system.version)) {
    const first = await Promise.all(Array.from({ length: 4 }, () => request('POST', '/events/publish', event(0))));
    assert.equal(new Set(first.map(r => r.event.id)).size, 1);
  }
  const result = await runLoad({ count, concurrency, maxP95Ms: Number(p95Arg ?? 1500), minRps: Number(rpsArg ?? 0), operation: async i => {
    const published = await request('POST', '/events/publish', event(i));
    assert.equal(published.accepted, true); assert.equal(published.event.eventId, event(i).id);
  } });
  console.error(JSON.stringify({ phase: 'publish', ...result }));
  if (!result.passed) return { ...state, count, load: result };
  // Multiple concurrent submissions of an already published ID must produce the same row.
  const duplicates = await Promise.all(Array.from({ length: 4 }, () => request('POST', '/events/publish', event(0))));
  assert.equal(new Set(duplicates.map(r => r.event.id)).size, 1);
  await request('POST', '/events/publish', { ...event(0), payload: { index: -1 } }, [409]);
  assert.equal(await db.eventRecord.count({ where: { topicName: state.topic } }), count);
  const rows = await db.eventSubscriptionDelivery.findMany({ where: { subscriptionId: state.subscriptionId } });
  assert.equal(rows.length, count); assert(rows.every(row => row.status === 'pending' && row.attempts === 0), 'Worker must be stopped during load');
  return { ...state, count, load: result };
}
async function drain() {
  const started = Date.now(); const timeout = Number(countArg ?? 180000);
  assert(Number.isFinite(timeout) && timeout > 0 && timeout <= 900000);
  while (Date.now() - started < timeout) {
    const rows = await db.eventSubscriptionDelivery.findMany({ where: { subscriptionId: state.subscriptionId } });
    assert.equal(rows.length, state.count);
    assert(rows.every(row => row.status !== 'dead'), 'Dead-letter during delivery');
    if (rows.every(row => row.status === 'delivered')) {
      assert(rows.every(row => row.attempts === 1), 'Unexpected retry or duplicate delivery');
      return { ...state, drainMs: Date.now() - started };
    }
    await delay(500);
  }
  throw Error('Queue drain deadline exceeded');
}
async function verify() {
  assert.equal((await request('GET', `/communities/${state.communityId}`)).title, state.title);
  assert.equal((await request('POST', `/backups/${state.backupId}/verify`, {})).valid, true);
  assert.equal(await redis.get(state.redisKey), state.id);
  for (const root of [process.env.STORAGE_LOCAL_ROOT, process.env.EXTENSIONS_ROOT]) assert.equal(await readFile(sentinelPath(root, state.id), 'utf8'), state.id);
  assert.equal(await db.eventRecord.count({ where: { topicName: state.topic } }), state.count);
  const rows = await db.eventSubscriptionDelivery.findMany({ where: { subscriptionId: state.subscriptionId } });
  assert.equal(rows.length, state.count); assert(rows.every(row => row.status === 'delivered' && row.attempts === 1));
  return state;
}
async function botSetup() {
  const communityId = state.communityId;
  await db.botAccount.create({ data: { id: 'rehearsal-discord', communityId, provider: 'discord', mode: 'webhook', externalId: 'fixture', tokenRef: 'env:DISCORD_BOT_TOKEN_REHEARSAL' } });
  await request('PUT', `/communities/${communityId}/message-core/forms`, { code: 'rehearsal', title: 'Rehearsal form', fields: [{ name: 'age', label: 'Возраст', type: 'number', required: true, min: 18 }] });
  await request('POST', `/communities/${communityId}/interactions/commands`, { name: 'rehearsal', provider: 'discord', description: 'Rehearsal form', type: 'slash_command', handler: 'form.start', metadata: { code: 'rehearsal' } });
  return { configured: true };
}
async function botTest() {
  const key = createPrivateKey({ key: Buffer.from(process.env.CAJEER_REHEARSAL_PRIVATE_KEY, 'base64'), format: 'der', type: 'pkcs8' });
  const base = 'http://bot-runtime:3101';
  const deadline = Date.now() + 60000;
  while (true) {
    try { const res = await fetch(base + '/runtime/health', { signal: AbortSignal.timeout(2000) }); if (res.ok) break; } catch { /* await startup */ }
    assert(Date.now() < deadline, 'Bot runtime readiness deadline'); await delay(500);
  }
  async function send(payload, expected = 200, bad = false) {
    const body = JSON.stringify(payload); const timestamp = String(Math.floor(Date.now() / 1000));
    const signature = bad ? '0'.repeat(128) : sign(null, Buffer.from(timestamp + body), key).toString('hex');
    const res = await fetch(base + '/runtime/webhooks/discord/rehearsal-discord', { method: 'POST', headers: { 'content-type': 'application/json', 'x-signature-ed25519': signature, 'x-signature-timestamp': timestamp }, body, signal: AbortSignal.timeout(10000) });
    assert.equal(res.status, expected); return res.json();
  }
  await send({ type: 1 }, 401, true);
  assert.equal((await send({ type: 1 })).type, 1);
  const common = { type: 2, channel_id: '1234567890', user: { id: '9876543210' } };
  const prompt = await send({ ...common, id: `${state.id}-start`, data: { name: 'rehearsal' } });
  assert.equal(prompt.type, 4); assert.equal(prompt.data.flags, 64); assert.match(prompt.data.content, /Возраст/);
  const answer = value => ({ ...common, id: `${state.id}-answer-${value}`, data: { name: 'answer', options: [{ name: 'value', value }] } });
  assert.match((await send(answer('15'))).data.content, /Возраст/);
  assert.match((await send(answer('25'))).data.content, /принята/);
  await send(answer('25'));
  const submissions = await db.messageFormSubmission.findMany({ where: { communityId: state.communityId } });
  assert.equal(submissions.length, 1); assert.deepEqual(submissions[0].normalized, { age: 25 });
  assert.equal(await db.botInboundUpdate.count({ where: { accountId: 'rehearsal-discord', status: 'handled' } }), 3);
  return { signedWebhook: true, invalidSignatureRejected: true, submissions: 1, normalized: { age: 25 } };
}
try {
  const operations = { prepare, publish, drain, verify, botSetup, botTest };
  assert(operations[action], 'Unknown workload action');
  console.log(JSON.stringify(await operations[action]()));
} finally { await db.$disconnect(); redis.disconnect(); }
