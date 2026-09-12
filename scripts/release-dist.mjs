#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { mkdir, readdir, readFile, stat, writeFile } from 'node:fs/promises';
import { basename, dirname, join, relative } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const version = (await readFile(join(root, 'VERSION'), 'utf8')).trim();
const outDir = join(root, 'dist-release');
const archive = join(outDir, `cajeerbots-${version}.zip`);
await mkdir(outDir, { recursive: true });

const include = [
  'apps', 'packages', 'prisma', 'contracts', 'docker', 'systemd', 'wiki',
  'package.json', 'pnpm-workspace.yaml', 'tsconfig.base.json', 'tsconfig.json',
  'README.md', 'CHANGELOG.md', 'LICENSE', 'NOTICE', 'SECURITY.md', 'VERSION',
  'docker-compose.yml', 'docker-compose.prod.yml', '.env.example', '.env.production.example'
];

const zip = spawnSync('zip', ['-qr', archive, ...include], { cwd: root, stdio: 'inherit' });
if (zip.status !== 0) process.exit(zip.status ?? 1);
const buffer = await readFile(archive);
const manifest = {
  schema: 'cajeerbots.release.manifest.v1',
  product: 'cajeerbots',
  version,
  channel: process.env.RELEASE_CHANNEL ?? 'alpha',
  createdAt: new Date().toISOString(),
  files: [{ path: basename(archive), sizeBytes: buffer.length, sha256: createHash('sha256').update(buffer).digest('hex') }],
};
await writeFile(join(outDir, `cajeerbots-${version}.manifest.json`), JSON.stringify(manifest, null, 2) + '\n');
console.log(JSON.stringify({ archive, manifest: join(outDir, `cajeerbots-${version}.manifest.json`) }, null, 2));
