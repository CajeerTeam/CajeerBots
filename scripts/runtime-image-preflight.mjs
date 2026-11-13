// Exercise the exact runtime image's maintenance tools and inventory.
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, statSync, chownSync, chmodSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { inventory } from './runtime-os-inventory.mjs';
const rows = inventory().trim().split('\n').map(row => row.split('\t')[0]);
for (const pkg of ['libc6','libssl3t64','libstdc++6','busybox-static','libatomic1','openssl']) assert(rows.includes(pkg), 'Missing runtime package: ' + pkg);
for (const pkg of ['util-linux','libmount1','libsystemd0','libudev1','perl-base','ncurses-bin','libsqlite3-0','libacl1']) assert(!rows.includes(pkg), 'Unexpected runtime package: ' + pkg);
assert.equal(process.getuid(), 0, 'Run this disposable test as root');
const dir = mkdtempSync(join(tmpdir(), 'runtime-preflight-'));
function run(cmd, args, options = {}) {
  const r = spawnSync(cmd, args, { ...options, encoding: null });
  assert.equal(r.status, 0, r.stderr?.toString() ?? String(r.error));
  return r.stdout;
}
try {
  const source = join(dir, 'source'), dest = join(dir, 'dest');
  mkdirSync(source); mkdirSync(dest);
  const sub = 'a'.repeat(110); mkdirSync(join(source, sub));
  const file = join(source, sub, 'fixture.txt');
  writeFileSync(file, 'backup fixture'); chownSync(file, 10001, 10001); chmodSync(file, 0o640);
  const archive = run('tar', ['-cpf','-','-C',source,'.']);
  run('tar', ['--numeric-owner','-xpf','-','-C',dest], { input: archive });
  const restored = join(dest,sub,'fixture.txt'), stat = statSync(restored);
  assert.equal(readFileSync(restored,'utf8'),'backup fixture');
  assert.equal(stat.uid,10001); assert.equal(stat.gid,10001); assert.equal(stat.mode & 0o777,0o640);
  // GNU tar compatibility: host-created archive, and an archive exported by this runtime.
  if (process.argv[2]) {
    const incoming = readFileSync(join(process.argv[2], 'gnu.tar'));
    const cross = join(dir,'cross'); mkdirSync(cross);
    run('tar',['--numeric-owner','-xpf','-','-C',cross],{input:incoming});
    const fixture = join(cross,'fixture.txt'), info = statSync(fixture);
    assert.equal(readFileSync(fixture,'utf8'),'gnu fixture');
    assert.equal(info.uid,10001); assert.equal(info.gid,10001); assert.equal(info.mode & 0o777,0o640);
    writeFileSync(join(process.argv[2],'runtime.tar'),archive);
  }
  console.log('Runtime inventory and backup/restore preflight OK');
} finally { rmSync(dir,{recursive:true,force:true}); }
