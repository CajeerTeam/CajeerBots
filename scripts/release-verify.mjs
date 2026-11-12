#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { verifyReleaseFiles } from './release-artifacts.mjs';
const [dir, version, keyFile] = process.argv.slice(2);
if (!dir || !version || !keyFile) throw new Error('Usage: release-verify.mjs DIRECTORY VERSION TRUSTED_PUBLIC_KEY.pem');
const prefix = `cajeerbots-${version}`;
const manifest = await verifyReleaseFiles(dir, `${prefix}.manifest.json`, `${prefix}.manifest.sig`, await readFile(keyFile), version);
console.log(JSON.stringify({ verified: true, version: manifest.version, commit: manifest.commit, files: manifest.files.length }));
