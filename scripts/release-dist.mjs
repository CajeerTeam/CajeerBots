#!/usr/bin/env node
// Build only from a clean committed tree; never package working-directory secrets or build caches.
import { createHash } from 'node:crypto';
import { access, mkdir, mkdtemp, readFile, rm, writeFile, copyFile, rename, lstat } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { artifactFingerprint, manifestDigest, signManifest, validateManifest, validVersion } from './release-artifacts.mjs';
const root = fileURLToPath(new URL('../', import.meta.url));
function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024, timeout: 120_000 });
  if (result.error || result.status !== 0) throw new Error(`${command} ${args[0]} failed: ${result.error?.message ?? result.status}`);
  return result.stdout;
}
const version = (await readFile(join(root, 'VERSION'), 'utf8')).trim();
if (!validVersion(version) || JSON.parse(await readFile(join(root, 'package.json'), 'utf8')).version !== version) throw new Error('Release version mismatch');
await access(join(root, 'pnpm-lock.yaml'));
if (run('git', ['status', '--porcelain', '--untracked-files=no']).trim()) throw new Error('Commit all tracked changes before packaging');
const commit = run('git', ['rev-parse', 'HEAD']).trim();
const signingKeyPath = process.env.RELEASE_SIGNING_KEY_FILE;
if (!signingKeyPath) throw new Error('RELEASE_SIGNING_KEY_FILE must point to an external Ed25519 private key');
const privateKey = await readFile(resolve(signingKeyPath));
const channel = process.env.RELEASE_CHANNEL ?? 'rc';
const prefix = `cajeerbots-${version}`;
const outDir = join(root, 'dist-release');
await mkdir(outDir, { recursive: true, mode: 0o700 });
const destination = join(outDir, prefix);
const staging = await mkdtemp(join(outDir, '.release-'));
try {
  const roots = new Set(['apps','packages','prisma','contracts','docker','systemd','wiki','scripts','examples','registry','docs','.github']);
  const top = new Set(['package.json','pnpm-lock.yaml','pnpm-workspace.yaml','.nvmrc','.npmrc','.dockerignore','.gitignore','eslint.config.mjs','prettier.config.cjs','vitest.config.ts','vitest.e2e.config.ts','tsconfig.base.json','tsconfig.json','README.md','CHANGELOG.md','ROADMAP.md','LICENSE','NOTICE','SECURITY.md','THIRD_PARTY_NOTICES.md','TRADEMARKS.md','CONTRIBUTING.md','CODE_OF_CONDUCT.md','VERSION','docker-compose.yml','docker-compose.prod.yml','.env.example','.env.production.example']);
  const tree = run('git', ['ls-tree', '-rz', 'HEAD']).split('\0').filter(Boolean);
  const paths = [];
  for (const entry of tree) {
    const tab = entry.indexOf('\t'); const meta = entry.slice(0, tab), path = entry.slice(tab + 1);
    if (!top.has(path) && !roots.has(path.split('/')[0]) && !/^RELEASE_VALIDATION_[0-9A-Za-z.-]+\.md$/.test(path)) continue;
    if (path.startsWith('prisma/generated/') || path.split('/').some((part) => ['node_modules','dist','coverage','.tmp','dist-release','.git'].includes(part) || (part.startsWith('.env') && !top.has(path)))) continue;
    if (/\.(?:pem|key|p12|pfx|dump|tar|zip|log)$/i.test(path) || /(?:^|\/)(?:id_rsa|id_ed25519|credentials\.json|secrets\.json)$/i.test(path)) throw new Error(`Sensitive or generated tracked file: ${path}`);
    if (!/^100(?:644|755) blob /.test(meta) || /[\r\n\0]/.test(path)) throw new Error(`Unsafe tracked file: ${path}`);
    paths.push(path);
  }
  for (const name of ['VERSION','package.json','pnpm-lock.yaml','.env.production.example','docker-compose.prod.yml']) if (!paths.includes(name)) throw new Error(`Missing tracked release input: ${name}`);
  const archiveName = `${prefix}.zip`;
  run('git', ['archive','--format=zip',`--prefix=${prefix}/`,`--output=${join(staging, archiveName)}`,commit,'--',...paths]);
  const names = [archiveName];
  // Optional prebuilt runtime image, CycloneDX SBOM, and actual rehearsal evidence.
  // Each is copied as a regular file and included in the signed manifest.
  for (const [variable, suffix] of [['RELEASE_IMAGE_FILE','.image.tar.gz'],['RELEASE_SBOM_FILE','.sbom.cdx.json'],['RELEASE_AUDIT_FILE','.audit.json'],['RELEASE_REHEARSAL_FILE','.rehearsal.json']]) {
    const source = process.env[variable]; if (!source) continue;
    const stat = await lstat(source); if (!stat.isFile() || stat.size === 0) throw new Error(`Invalid ${variable}`);
    const name = prefix + suffix; await copyFile(source, join(staging, name)); names.push(name);
  }
  if (process.env.RELEASE_REQUIRE_EVIDENCE === '1' && names.length !== 5) throw new Error('Image, CycloneDX SBOM, audit and rehearsal evidence required');
  if (process.env.RELEASE_REHEARSAL_FILE) {
    const report = JSON.parse(await readFile(join(staging, prefix + '.rehearsal.json'),'utf8'));
    if (report.passed !== true || !report.phases?.length || report.phases.some(p => p.passed !== true) || !report.phases.some(p => p.name === 'upgrade-rc5-to-rc6') || !report.phases.some(p => p.name === 'verify-restored-data')) throw new Error('Full RC5 to RC6 rehearsal must pass');
  }
  const files = await Promise.all(names.sort().map(name => artifactFingerprint(staging, name)));
  const manifest = { schema: 'cajeerbots.release.manifest.v1', product: 'cajeerbots', version, channel, commit, createdAt: run('git', ['show','-s','--format=%cI',commit]).trim(), files };
  manifest.digest = manifestDigest(manifest); validateManifest(manifest);
  const bytes = Buffer.from(JSON.stringify(manifest, null, 2) + '\n');
  await writeFile(join(staging, `${prefix}.manifest.json`), bytes, { flag: 'wx' });
  await writeFile(join(staging, `${prefix}.manifest.sig`), signManifest(bytes, privateKey), { flag: 'wx' });
  await writeFile(join(staging, 'SHA256SUMS'), [...files.map(f => `${f.sha256}  ${f.path}`), `${createHash('sha256').update(bytes).digest('hex')}  ${prefix}.manifest.json`].join('\n') + '\n', { flag: 'wx' });
  try { await lstat(destination); throw new Error('Release output already exists'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  await rename(staging, destination);
  console.log(JSON.stringify({ directory: destination, version, commit, files: names }));
} finally { await rm(staging, { recursive: true, force: true }); }
