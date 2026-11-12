#!/usr/bin/env node
// Host-side operations for the standard single-instance production Compose stack.
import { createHash } from 'node:crypto';
import { createReadStream, existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, closeSync, fsyncSync, readFileSync, renameSync, rmSync, writeFileSync, copyFileSync, chmodSync, realpathSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, isAbsolute, join, resolve, relative } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';

import { verifyReleaseFiles } from './release-artifacts.mjs';
import { requireStableManifest, readStableEvidence, verifyImageArchive, stablePrefix, stableVersion, stableSuffixes } from './stable-evidence.mjs';
import { capacitySql, pilotLimits } from './pilot-policy.mjs';
const root = fileURLToPath(new URL('../', import.meta.url));
const files = ['database.dump', 'redis.tar', 'storage.tar', 'extensions.tar', 'env.production'];
const services = ['api', 'bot-runtime', 'worker', 'migrate'];
const timeout = 30 * 60 * 1000;
function assert(condition, message) { if (!condition) throw new Error(message); }
export async function fingerprint(path) {
  const stat = lstatSync(path);
  assert(stat.isFile() && !stat.isSymbolicLink(), `Not a regular file: ${path}`);
  const hash = createHash('sha256'); let bytes = 0;
  for await (const chunk of createReadStream(path)) { hash.update(chunk); bytes += chunk.length; }
  assert(bytes === stat.size, `File changed while reading: ${path}`);
  return { bytes, sha256: hash.digest('hex') };
}
export async function verifyBackup(dir) {
  const manifestPath = join(dir, 'manifest.json');
  assert(lstatSync(manifestPath).isFile(), 'Manifest must be a regular file');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  assert(manifest.format === 'cajeerbots.instance-backup.v1', 'Unsupported backup format');
  assert(typeof manifest.project === 'string' && typeof manifest.image === 'string' && /^sha256:[a-f0-9]{64}$/.test(manifest.imageId), 'Invalid backup identity');
  assert(typeof manifest.version === 'string' && manifest.files && Object.keys(manifest.files).length === files.length, 'Invalid backup manifest');
  for (const name of files) {
    const actual = await fingerprint(join(dir, name)); const expected = manifest.files[name];
    assert(expected && actual.bytes === expected.bytes && actual.sha256 === expected.sha256 && actual.bytes > 0, `Backup checksum mismatch: ${name}`);
  }
  return manifest;
}
function run(args, { env = process.env, input, output, capture = false } = {}) {
  let infd, outfd;
  try {
    if (input) infd = openSync(input, 'r');
    if (output) outfd = openSync(output, 'wx', 0o600);
    const result = spawnSync('docker', args, { cwd: root, env, stdio: [infd ?? 'ignore', outfd ?? (capture ? 'pipe' : 'inherit'), 'inherit'], encoding: 'utf8', timeout, maxBuffer: 16 * 1024 * 1024 });
    if (result.error || result.status !== 0) throw new Error(`Docker operation failed (${args.slice(0, 2).join(' ')}): ${result.error?.message ?? result.status ?? result.signal}`);
    if (outfd !== undefined) fsyncSync(outfd);
    return capture ? result.stdout.trim() : undefined;
  } finally { if (infd !== undefined) closeSync(infd); if (outfd !== undefined) closeSync(outfd); }
}
function compose(project, envFile, image) {
  assert(/^[a-z0-9][a-z0-9_-]*$/.test(project), 'Invalid Compose project name');
  assert(isAbsolute(envFile) && existsSync(envFile), 'Environment file not found');
  const env = { ...process.env, CAJEER_ENV_FILE: envFile, CAJEER_IMAGE: image };
  // No ambient Compose files or profiles; project and file are explicit.
  delete env.COMPOSE_PROFILES;
  const args = ['compose', '--project-name', project, '--env-file', envFile, '-f', join(root, 'docker-compose.prod.yml')];
  return (cmd, opts = {}) => run([...args, ...cmd], { ...opts, env });
}
function config(c) {
  const cfg = JSON.parse(c(['config', '--format', 'json'], { capture: true }));
  const app = cfg.services.api.environment;
  const pg = cfg.services.postgres.environment;
  const db = new URL(app.DATABASE_URL);
  const redis = new URL(app.REDIS_URL);
  assert(db.hostname === 'postgres' && (!db.port || db.port === '5432') && decodeURIComponent(db.pathname.slice(1)) === pg.POSTGRES_DB && decodeURIComponent(db.username) === pg.POSTGRES_USER && decodeURIComponent(db.password) === pg.POSTGRES_PASSWORD && (!db.searchParams.get('schema') || db.searchParams.get('schema') === 'public'), 'Only the bundled PostgreSQL/public schema is supported');
  assert(redis.hostname === 'redis' && (!redis.port || redis.port === '6379') && !redis.username && !redis.password && ['', '/', '/0'].includes(redis.pathname), 'Only the bundled Redis database 0 is supported');
  assert(app.QUEUE_DRIVER === 'redis' || app.QUEUE_DRIVER === 'postgres', 'External queue requires separate backup');
  assert(app.STORAGE_DRIVER === 'local', 'External/S3 storage requires a separate backup procedure');
  return cfg;
}
function imageId(image) { return run(['image', 'inspect', '--format', '{{.Id}}', image], { capture: true }); }
function currentImage(c, cfg) {
  const id = c(['ps', '-q', 'api'], { capture: true });
  assert(id && !id.includes('\n'), 'Expected one running API container');
  const actual = JSON.parse(run(['inspect', id], { capture: true }))[0];
  assert(actual.Image === imageId(cfg.services.api.image), 'Running API image differs from configured image');
  const version = c(['exec', '-T', 'api', 'node', '-p', "require('fs').readFileSync('VERSION','utf8').trim()"], { capture: true });
  // The environment file must match the currently running configuration, especially encryption keys.
  for (const name of ['api', 'bot-runtime', 'worker', 'postgres', 'redis']) {
    const container = c(['ps', '-q', name], { capture: true });
    assert(container && !container.includes('\n'), `Expected one running ${name} container`);
    const inspected = JSON.parse(run(['inspect', container], { capture: true }))[0];
    if (services.includes(name)) assert(inspected.Image === actual.Image, 'Mixed application versions');
    const current = Object.fromEntries(inspected.Config.Env.map((entry) => [entry.slice(0, entry.indexOf('=')), entry.slice(entry.indexOf('=') + 1)]));
    for (const [key, value] of Object.entries(cfg.services[name].environment ?? {})) assert(current[key] === String(value), `Configuration drift: ${name}.${key}`);
  }
  return { version, image: cfg.services.api.image, imageId: actual.Image };
}
function quiesce(c) {
  c(['stop', '-t', '40', 'api', 'bot-runtime', 'worker', 'migrate']);
  assert(!c(['ps', '-q', '--status', 'running', ...services], { capture: true }), 'Application writers are still running');
  for (const name of ['api', 'bot-runtime', 'worker']) {
    const id = c(['ps', '-aq', name], { capture: true });
    const info = JSON.parse(run(['inspect', id], { capture: true }))[0];
    assert(info.State.ExitCode === 0 || (name === 'api' && info.State.ExitCode === 143), `Unclean stop of ${name}: ${info.State.ExitCode}`);
  }
  c(['stop', '-t', '40', 'redis']);
  assert(!c(['ps', '-q', '--status', 'running', 'redis'], { capture: true }), 'Redis is still running');
  const redisId = c(['ps', '-aq', 'redis'], { capture: true });
  assert(JSON.parse(run(['inspect', redisId], { capture: true }))[0].State.ExitCode === 0, 'Redis did not shut down cleanly');
}
function volume(c, name, dir, file, restore = false) {
  const args = ['run', '--rm', '--no-deps', '-T', '--user', '0', '--entrypoint', 'tar', name];
  if (restore) c([...args, '--numeric-owner', '-xpf', '-', '-C', dir], { input: file });
  else c([...args, '-cpf', '-', '-C', dir, '.'], { output: file });
}
function fsync(path) { const fd = openSync(path, 'r'); try { fsyncSync(fd); } finally { closeSync(fd); } }
async function backup(c, project, envFile, destination) {
  const cfg = config(c);
  const identity = currentImage(c, cfg);
  assert(!existsSync(destination), 'Backup destination already exists');
  const parent = dirname(destination); mkdirSync(parent, { recursive: true, mode: 0o700 });
  safeDestination(join(realpathSync(parent), 'backup')); 
  const staging = mkdtempSync(join(parent, '.cajeer-backup-'));
  try {
    copyFileSync(envFile, join(staging, 'env.production')); chmodSync(join(staging, 'env.production'), 0o600);
    quiesce(c);
    c(['exec', '-T', 'postgres', 'sh', '-ec', 'exec pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom --no-owner --no-acl'], { output: join(staging, 'database.dump') });
    // Read the dump catalog with the same PostgreSQL toolchain before publishing.
    c(['exec', '-T', 'postgres', 'pg_restore', '--list'], { input: join(staging, 'database.dump'), capture: true });
    volume(c, 'redis', '/data', join(staging, 'redis.tar'));
    volume(c, 'api', '/var/lib/cajeerbots/storage', join(staging, 'storage.tar'));
    volume(c, 'api', '/var/lib/cajeerbots/extensions', join(staging, 'extensions.tar'));
    const manifest = { format: 'cajeerbots.instance-backup.v1', createdAt: new Date().toISOString(), project, ...identity, files: {} };
    for (const name of files) { fsync(join(staging, name)); manifest.files[name] = await fingerprint(join(staging, name)); }
    writeFileSync(join(staging, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    fsync(join(staging, 'manifest.json')); await verifyBackup(staging); fsync(staging);
    assert(!existsSync(destination), 'Backup destination appeared during backup');
    renameSync(staging, destination); fsync(parent);
    console.log(JSON.stringify({ backup: destination, version: identity.version, stopped: true }));
    return manifest;
  } finally { rmSync(staging, { recursive: true, force: true }); }
}
function assertFresh(c, project) {
  assert(!c(['ps', '-aq'], { capture: true }), 'Restore target already has containers');
  assert(!run(['volume', 'ls', '-q', '--filter', `label=com.docker.compose.project=${project}`], { capture: true }), 'Restore target already has volumes');
  for (const name of ['postgres_data', 'redis_data', 'cajeer_storage', 'cajeer_extensions']) {
    assert(!run(['volume', 'ls', '-q', '--filter', `name=^${project}_${name}$`], { capture: true }), 'Restore target volume already exists');
  }
}
async function restore(project, dir) {
  const manifest = await verifyBackup(dir);
  assert(project !== manifest.project, 'Restore must use a different project');
  assert(imageId(manifest.image) === manifest.imageId, 'Original application image is not available or changed');
  const c = compose(project, join(dir, 'env.production'), manifest.image);
  config(c); assertFresh(c, project);
  // No application services or migration/seed run until the entire restore succeeds.
  c(['up', '-d', '--wait', '--wait-timeout', '120', 'postgres']);
  const tables = c(['exec', '-T', 'postgres', 'sh', '-ec', `exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')"`], { capture: true });
  assert(tables === '0', 'Restore target database is not empty');
  c(['exec', '-T', 'postgres', 'sh', '-ec', 'exec pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --exit-on-error --single-transaction --no-owner --no-acl'], { input: join(dir, 'database.dump') });
  volume(c, 'redis', '/data', join(dir, 'redis.tar'), true);
  volume(c, 'api', '/var/lib/cajeerbots/storage', join(dir, 'storage.tar'), true);
  volume(c, 'api', '/var/lib/cajeerbots/extensions', join(dir, 'extensions.tar'), true);
  c(['up', '-d', '--wait', '--wait-timeout', '120', 'redis']);
  console.log(JSON.stringify({ restored: true, project, version: manifest.version, applicationStopped: true }));
}
function start(c, version, beforeStart = () => {}) {
  assert(!c(['ps', '-q', '--status', 'running', ...services], { capture: true }), 'Stop application writers before migrations');
  const imageVersion = c(['run', '--rm', '--no-deps', '-T', '--entrypoint', 'node', 'api', '-p', "require('fs').readFileSync('VERSION','utf8').trim()"], { capture: true });
  assert(imageVersion === version, 'Unexpected application image version');
  // Run schema migrations explicitly; do not seed during update/restore.
  c(['up', '-d', '--wait', '--wait-timeout', '120', 'postgres', 'redis']);
  c(['run', '--rm', '--no-deps', '-T', 'migrate', 'pnpm', 'prisma:deploy']);
  beforeStart();
  c(['up', '-d', '--no-deps', '--no-build', '--pull', 'never', '--wait', '--wait-timeout', '240', 'api', 'bot-runtime', 'worker']);
  const report = c(['exec', '-T', 'api', 'node', '-e', `fetch('http://127.0.0.1:3000/api/system/report',{headers:{'x-api-key':process.env.CAJEER_ADMIN_TOKEN},signal:AbortSignal.timeout(5000)}).then(async r=>{if(!r.ok)throw Error('report '+r.status);console.log((await r.json()).version)}).catch(()=>process.exit(1))`], { capture: true });
  assert(report === version, `Unexpected running version: ${report}`);
}
function safeDestination(path) {
  const rel = relative(root, path);
  assert((rel === '..' || rel.startsWith('../')) && !isAbsolute(rel), 'Backup must be outside the repository/build context');
}
async function deployPilot(project, envFile, bundle, publicKeyFile, communities, bots, upgrade) {
  const limits = pilotLimits(communities,bots);
  const envStat = lstatSync(envFile);
  assert(envStat.isFile() && (envStat.mode & 0o077) === 0, 'Pilot env must be a regular private file (0600)');
  const staging = mkdtempSync(join(tmpdir(), 'cajeer-verified-release-'));
  try {
    // Verify a private snapshot, then load those exact bytes, not the mutable source directory.
    for (const suffix of [...stableSuffixes,'.manifest.json','.manifest.sig']) {
      const name = stablePrefix + suffix;
      assert(lstatSync(join(bundle,name)).isFile(), 'Release artifact must be a regular file');
      copyFileSync(join(bundle,name),join(staging,name));
    }
    const manifest = await verifyReleaseFiles(staging,stablePrefix+'.manifest.json',stablePrefix+'.manifest.sig',readFileSync(publicKeyFile),stableVersion);
    requireStableManifest(manifest);
    const id = await readStableEvidence(staging,manifest.commit);
    const archive = join(staging,stablePrefix+'.image.tar.gz'); verifyImageArchive(archive,id,manifest.commit);
    const image = 'cajeerbots:pilot-'+id.slice(7);
    const c = compose(project,envFile,image); const cfg = config(c);
    let old;
    if (upgrade) {
      safeDestination(upgrade.destination);
      old = compose(project,envFile,upgrade.oldImage);
      assert(currentImage(old,config(old)).version==='1.0.0-rc.6','Pilot upgrade requires a running RC6 instance');
      const count = old(['exec','-T','postgres','sh','-ec','exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1 -c "$1"','pilot',`SELECT (SELECT count(*) FROM communities)::text || ',' || (SELECT count(*) FROM bot_accounts)::text`],{capture:true}).split(',').map(Number);
      assert(count.length===2 && count.every(Number.isSafeInteger) && count[0]<=limits.maxCommunities && count[1]<=limits.maxBotAccounts,'Current instance exceeds pilot limits');
    } else assertFresh(c,project);
    assert(cfg.services.api.ports?.every(p=>p.host_ip==='127.0.0.1') && cfg.services['bot-runtime'].ports?.every(p=>p.host_ip==='127.0.0.1'), 'Pilot ports must be loopback only');
    run(['load','--input',archive]); assert(imageId(id) === id,'Loaded image ID mismatch'); run(['tag',id,image]);
    const actualVersion = c(['run','--rm','--no-deps','-T','--entrypoint','node','api','-p',"require('fs').readFileSync('VERSION','utf8').trim()"],{capture:true});
    assert(actualVersion===stableVersion,'Wrong image version');
    if (upgrade) await backup(old,project,envFile,upgrade.destination);
    const setCapacity = () => {
      assert(imageId(image)===id,'Pilot image tag changed');
      c(['exec','-T','postgres','sh','-ec','exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "$1"','pilot',capacitySql(communities,bots)]);
    };
    try {
      if (upgrade) start(c,stableVersion,setCapacity);
      else {
        c(['up','-d','--wait','--wait-timeout','120','postgres','redis']);
        c(['run','--rm','--no-deps','-T','migrate','sh','-ec','pnpm prisma:deploy && pnpm prisma:seed']);
        setCapacity();
        c(['up','-d','--no-deps','--no-build','--pull','never','--wait','--wait-timeout','240','api','bot-runtime','worker']);
      }
      assert(currentImage(c,cfg).imageId===id,'Running pilot image differs');
      const status = JSON.parse(c(['exec','-T','api','node','scripts/pilot-probe.mjs'],{capture:true}));
      assert(status.passed===true,'Pilot health/capacity gate failed');
      console.log(JSON.stringify({deployed:true,project,image,imageId:id,commit:manifest.commit,status,...(upgrade?{backup:upgrade.destination}: {})}));
    } catch(error) { c(['stop','-t','40',...services]); throw error; }
  } finally { rmSync(staging,{recursive:true,force:true}); }
}
export async function main(argv) {
  const [action, project, envArg, image, arg, targetVersion] = argv;
  if (action === 'verify' && project && argv.length === 2) { console.log(JSON.stringify(await verifyBackup(resolve(project)), null, 2)); return; }
  assert(['backup', 'restore', 'start', 'upgrade', 'pilot-deploy', 'pilot-upgrade', 'pilot-status', 'pilot-pause'].includes(action) && project && /^[a-z0-9][a-z0-9_-]*$/.test(project), 'Usage: instance-ops.mjs backup PROJECT ENV IMAGE DEST | restore PROJECT BACKUP | start PROJECT ENV IMAGE VERSION | upgrade PROJECT ENV OLD_IMAGE DEST NEW_IMAGE VERSION | verify BACKUP | pilot-deploy PROJECT ENV BUNDLE PUBLIC_KEY MAX_COMMUNITIES MAX_BOTS | pilot-upgrade PROJECT ENV OLD_IMAGE BACKUP BUNDLE PUBLIC_KEY MAX_COMMUNITIES MAX_BOTS | pilot-status/pilot-pause PROJECT ENV IMAGE');
  const lock = join(tmpdir(), `cajeerbots-instance-${process.getuid?.() ?? 'user'}-${project}.lock`);
  mkdirSync(lock, { mode: 0o700 });
  try {
    if (action === 'pilot-upgrade') { assert(argv.length===9,'pilot-upgrade PROJECT ENV OLD_IMAGE BACKUP BUNDLE PUBLIC_KEY MAX_COMMUNITIES MAX_BOTS'); await deployPilot(project,resolve(envArg),resolve(targetVersion),resolve(argv[6]),argv[7],argv[8],{oldImage:image,destination:resolve(arg)}); return; }
    if (action === 'pilot-deploy') { assert(argv.length === 7,'pilot-deploy PROJECT ENV BUNDLE PUBLIC_KEY MAX_COMMUNITIES MAX_BOTS'); await deployPilot(project,resolve(envArg),resolve(image),resolve(arg),targetVersion,argv[6]); return; }
    if (action === 'pilot-status' || action === 'pilot-pause') {
      assert(argv.length === 4,'pilot-status/pilot-pause PROJECT ENV IMAGE'); const c=compose(project,resolve(envArg),image); const cfg=config(c);
      if(action==='pilot-pause') { c(['stop','-t','40',...services]); console.log(JSON.stringify({paused:true,project})); return; }
      assert(currentImage(c,cfg).version===stableVersion,'Pilot running version mismatch');
      const status=JSON.parse(c(['exec','-T','api','node','scripts/pilot-probe.mjs'],{capture:true})); console.log(JSON.stringify(status)); if(!status.passed) throw Error('Pilot gate failed'); return;
    }
    if (action === 'restore') { assert(argv.length === 3, 'restore PROJECT BACKUP'); await restore(project, resolve(envArg)); return; }
    assert(envArg && image && arg, 'Missing environment/image/argument');
    const envFile = resolve(envArg); const c = compose(project, envFile, image);
    if (action === 'start') { assert(argv.length === 5, 'start PROJECT ENV IMAGE VERSION'); config(c); imageId(image); start(c, arg); return; }
    const dest = resolve(arg); safeDestination(dest);
    if (action === 'backup') { assert(argv.length === 5, 'backup PROJECT ENV IMAGE DEST'); await backup(c, project, envFile, dest); return; }
    const newImage = targetVersion; const version = argv[6];
    assert(argv.length === 7 && newImage && version, 'upgrade PROJECT ENV OLD_IMAGE DEST NEW_IMAGE VERSION');
    const newId = imageId(newImage); const next = compose(project, envFile, newImage); config(next);
    const actualVersion = next(['run', '--rm', '--no-deps', '-T', '--entrypoint', 'node', 'api', '-p', "require('fs').readFileSync('VERSION','utf8').trim()"], { capture: true });
    assert(actualVersion === version && newId !== imageId(image), 'Wrong or unchanged target image');
    await backup(c, project, envFile, dest);
    try { assert(imageId(newImage) === newId, 'Target image changed'); start(next, version); }
    catch (error) { next(['stop', '-t', '40', ...services]); throw error; }
    console.log(JSON.stringify({ upgraded: true, version, backup: dest }));
  } finally { rmSync(lock, { recursive: true, force: true }); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch((error) => { console.error(`Instance operation failed: ${error.message}. Check service state before resuming; no automatic destructive rollback.`); process.exitCode = 1; });
}
