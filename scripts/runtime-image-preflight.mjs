// Verify the shell-free production image, with its real OS inventory.
import assert from 'node:assert/strict';
import { existsSync, accessSync, constants } from 'node:fs';
import { inventory } from './runtime-os-inventory.mjs';
const rows = inventory().trim().split('\n').map(row => row.split('\t')[0]);
for (const pkg of ['libc6','libssl3t64','libstdc++6','libatomic1','openssl']) assert(rows.includes(pkg), 'Missing runtime package: ' + pkg);
for (const pkg of ['busybox-static','util-linux','libmount1','libsystemd0','libudev1','perl-base','ncurses-bin','libsqlite3-0','libacl1']) assert(!rows.includes(pkg), 'Unexpected runtime package: ' + pkg);
for (const path of ['/bin/sh','/usr/bin/sh','/usr/bin/busybox','/bin/tar','/usr/bin/tar','/usr/local/bin/npm','/usr/local/bin/pnpm']) assert(!existsSync(path), 'Unexpected runtime executable: ' + path);
assert.equal(process.getuid(),10001);
assert(existsSync('prisma/seed.js'));
accessSync('/var/lib/cajeerbots/storage', constants.W_OK);
accessSync('/var/lib/cajeerbots/extensions', constants.W_OK);
console.log('Shell-free runtime inventory and writable storage OK');
