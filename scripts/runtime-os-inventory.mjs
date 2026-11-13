// Inventory the actual runtime package database, including Distroless status.d.
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export function inventory(root = '/') {
  const dir = join(root, 'var/lib/dpkg');
  const records = new Map();
  const files = [];
  if (existsSync(join(dir, 'status'))) files.push(join(dir, 'status'));
  if (existsSync(join(dir, 'status.d'))) {
    for (const entry of readdirSync(join(dir, 'status.d'), { withFileTypes: true })) {
      if (entry.isFile() && !entry.name.endsWith('.md5sums')) files.push(join(dir, 'status.d', entry.name));
    }
  }
  for (const file of files) {
    for (const paragraph of readFileSync(file, 'utf8').trim().split(/\n\s*\n/)) {
      if (!paragraph.trim()) continue;
      const field = name => paragraph.match(new RegExp('^' + name + ': ([^\\n]+)$', 'm'))?.[1].trim();
      const name = field('Package'), version = field('Version'), arch = field('Architecture'), status = field('Status');
      if (!name || !version || !arch) throw Error('Incomplete Debian package record: ' + file);
      if (status && status !== 'install ok installed') continue;
      if (!/^[a-z0-9.+-]+$/.test(name) || !/^[a-z0-9]+$/.test(arch) || /[\s]/.test(version)) throw Error('Invalid Debian package identity: ' + name);
      const key = name + ':' + arch;
      const row = [name, version, arch].join('\t');
      if (records.has(key) && records.get(key) !== row) throw Error('Conflicting package versions: ' + key);
      records.set(key, row);
    }
  }
  if (!records.size) throw Error('Missing Debian package inventory');
  return [...records.values()].sort().join('\n') + '\n';
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) process.stdout.write(inventory());
