#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { access, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const version = (await readFile(join(root, 'VERSION'), 'utf8')).trim();
const outDir = join(root, 'dist-release');
const archive = join(outDir, `cajeerbots-${version}.zip`);
await access(join(root, "pnpm-lock.yaml"));
await mkdir(outDir, { recursive: true });
await rm(archive, { force: true });

const include = [
  'apps', 'packages', 'prisma', 'contracts', 'docker', 'systemd', 'wiki',
  'package.json', 'pnpm-lock.yaml', 'pnpm-workspace.yaml', '.nvmrc', '.npmrc', '.dockerignore', 'scripts', 'examples', 'registry', 'eslint.config.mjs', 'vitest.config.ts', 'vitest.e2e.config.ts', 'tsconfig.base.json', 'tsconfig.json',
  'README.md', 'CHANGELOG.md', 'ROADMAP.md', 'RELEASE_VALIDATION_1.0.0-rc.5.md', 'docs', 'LICENSE', 'NOTICE', 'SECURITY.md', 'VERSION',
  'docker-compose.yml', 'docker-compose.prod.yml', '.env.example', '.env.production.example'
];

const zip = spawnSync('zip', ['-qr', archive, ...include, '-x', '*/node_modules/*', '*/dist/*', 'prisma/generated/*', '*/.env', '*/.env.*'], { cwd: root, stdio: 'inherit' });
if (zip.status !== 0) process.exit(zip.status ?? 1);
const buffer = await readFile(archive);
const manifest = {
  schema: 'cajeerbots.release.manifest.v1',
  product: 'cajeerbots',
  version,
  channel: process.env.RELEASE_CHANNEL ?? 'rc',
  createdAt: new Date().toISOString(),
  files: [{ path: basename(archive), sizeBytes: buffer.length, sha256: createHash('sha256').update(buffer).digest('hex') }],
};
await writeFile(join(outDir, `cajeerbots-${version}.manifest.json`), JSON.stringify(manifest, null, 2) + '\n');
console.log(JSON.stringify({ archive, manifest: join(outDir, `cajeerbots-${version}.manifest.json`) }, null, 2));
