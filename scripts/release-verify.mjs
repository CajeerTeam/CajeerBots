#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { verifyReleaseFiles } from './release-artifacts.mjs';
import { requireStableManifest, readStableEvidence, verifyImageArchive, stableVersion } from './stable-evidence.mjs';
import { join } from 'node:path';
const [dir, version, keyFile] = process.argv.slice(2);
if (!dir || !version || !keyFile) throw new Error('Usage: release-verify.mjs DIRECTORY VERSION TRUSTED_PUBLIC_KEY.pem');
const prefix = `cajeerbots-${version}`;
const manifest = await verifyReleaseFiles(dir, `${prefix}.manifest.json`, `${prefix}.manifest.sig`, await readFile(keyFile), version);
if (version === stableVersion) {
  requireStableManifest(manifest);
  verifyImageArchive(join(dir, prefix + '.image.tar.gz'), await readStableEvidence(dir, manifest.commit), manifest.commit);
}
console.log(JSON.stringify({ verified: true, version: manifest.version, commit: manifest.commit, files: manifest.files.length }));
