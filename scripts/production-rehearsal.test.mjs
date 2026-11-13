import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { mkdtemp, writeFile, rm, symlink, readFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fingerprint, verifyBackup } from './instance-ops.mjs';
import { runLoad } from './rehearsal-load.mjs';

async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'rc5-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const manifest = { format: 'cajeerbots.instance-backup.v1', project: 'test', image: 'cajeerbots:rc4', imageId: `sha256:${'a'.repeat(64)}`, version: '1.0.0-rc.4', files: {} };
  for (const file of ['database.dump','redis.tar','storage.tar','extensions.tar','env.production']) {
    await writeFile(join(dir, file), `content:${file}`);
    manifest.files[file] = await fingerprint(join(dir, file));
  }
  await writeFile(join(dir, 'manifest.json'), JSON.stringify(manifest));
  return { dir, manifest };
}
test('backup verification verifies all five files and detects corruption', async t => {
  const { dir, manifest } = await fixture(t);
  assert.deepEqual(await verifyBackup(dir), manifest);
  await writeFile(join(dir, 'redis.tar'), 'corrupt');
  await assert.rejects(verifyBackup(dir), /checksum mismatch: redis.tar/);
});
test('missing file prevents restore preflight', async t => {
  const { dir } = await fixture(t); await rm(join(dir, 'database.dump'));
  await assert.rejects(verifyBackup(dir), /ENOENT/);
});
test('symlink cannot substitute backup artifact', async t => {
  const { dir } = await fixture(t); await rm(join(dir, 'storage.tar'));
  await symlink(join(dir, 'extensions.tar'), join(dir, 'storage.tar'));
  await assert.rejects(verifyBackup(dir), /Not a regular file/);
});
test('manifest cannot omit required file or change path', async t => {
  const { dir, manifest } = await fixture(t);
  delete manifest.files['database.dump']; manifest.files['../other'] = { bytes: 1, sha256: 'x' };
  await writeFile(join(dir, 'manifest.json'), JSON.stringify(manifest));
  await assert.rejects(verifyBackup(dir), /checksum mismatch: database.dump/);
});
test('load runner uses bounded real HTTP concurrency and records every request', async t => {
  let active = 0, max = 0; const seen = new Set();
  const server = createServer(async (req, res) => { active++; max = Math.max(max, active); seen.add(req.url); await delay(15); active--; res.end('ok'); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const report = await runLoad({ count: 40, concurrency: 4, maxP95Ms: 2000, minRps: 1, operation: async i => { const r = await fetch(`http://127.0.0.1:${server.address().port}/${i}`); assert.equal(await r.text(), 'ok'); } });
  assert.equal(report.succeeded, 40); assert.equal(seen.size, 40); assert(max <= 4 && max > 1); assert(report.passed); assert(report.p99Ms >= report.p95Ms && report.p95Ms >= report.p50Ms);
});
test('failed requests and latency thresholds fail the load gate', async () => {
  const error = await runLoad({ count: 5, concurrency: 2, maxP95Ms: 1000, operation: i => { if (i === 2) throw Error('bad'); } });
  assert.equal(error.failed, 1); assert.equal(error.passed, false); assert.equal(error.errors[0].index, 2);
  const slow = await runLoad({ count: 3, concurrency: 1, maxP95Ms: 1, operation: () => delay(10) });
  assert.equal(slow.passed, false);
  const lowRps = await runLoad({ count: 1, concurrency: 1, maxP95Ms: 1000, minRps: 10000, operation: () => delay(10) });
  assert.equal(lowRps.passed, false);
  await assert.rejects(runLoad({ count: 0, concurrency: 1, maxP95Ms: 1, operation: () => {} }), /Invalid/);
});

test('local storage atomically writes and rejects traversal and symlink objects', async t => {
  // Compile the actual TS source without requiring a workspace build.
  const { pathToFileURL } = await import('node:url');
  const ts = (await import(process.env.TS_COMPILER_PATH ? pathToFileURL(process.env.TS_COMPILER_PATH).href : 'typescript')).default;
  const dir = await mkdtemp(join(tmpdir(), 'rc5-storage-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const src = await readFile(new URL('../packages/storage/src/local-storage.ts', import.meta.url), 'utf8');
  const modulePath = join(dir, 'local-storage.mjs');
  await writeFile(modulePath, ts.transpileModule(src, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText);
  const { LocalStorageDriver } = await import(pathToFileURL(modulePath).href);
  const root = join(dir, 'storage'); const driver = new LocalStorageDriver(root);
  await driver.put('backup/test.json', Buffer.from('first'));
  await driver.put('backup/test.json', Buffer.from('second'));
  assert.equal(Buffer.from(await driver.get('backup/test.json')).toString(), 'second');
  await assert.rejects(driver.put('../outside', Buffer.from('bad')), /Unsafe/);
  await assert.rejects(driver.get('/etc/passwd'), /Unsafe/);
  await mkdir(join(dir, 'outside'));
  await symlink(join(dir, 'outside'), join(root, 'link'));
  await assert.rejects(driver.put('link/escape', Buffer.from('bad')), /Unsafe/);
  await symlink(join(dir, 'local-storage.mjs'), join(root, 'backup', 'symlink'));
  await assert.rejects(driver.get('backup/symlink'), /Unsafe/);
  await driver.delete('backup/test.json'); await driver.delete('backup/test.json');
  await assert.rejects(driver.get('backup/test.json'), /ENOENT/);
});

test('workload ESM imports resolve with the installed dependencies without connecting to services', () => {
  const script = fileURLToPath(new URL('./rehearsal-workload.mjs', import.meta.url));
  const result = spawnSync(process.execPath, [script, '--check-imports'], {
    encoding: 'utf8',
    timeout: 10000,
    env: { ...process.env, CAJEER_REHEARSAL_ISOLATED: '0', DATABASE_URL: '', REDIS_URL: '', CAJEER_ADMIN_TOKEN: '' },
  });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Rehearsal workload imports OK/);
  const denied = spawnSync(process.execPath, [script, 'prepare'], {
    encoding: 'utf8', timeout: 10000,
    env: { ...process.env, CAJEER_REHEARSAL_ISOLATED: '0' },
  });
  assert.notEqual(denied.status, 0);
  assert.match(denied.stderr, /Only disposable rehearsal instances/);
});
