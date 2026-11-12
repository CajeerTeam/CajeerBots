import { mkdir, readFile, rm, writeFile, lstat, rename } from 'node:fs/promises';
import { dirname, resolve, sep } from 'node:path';
import { randomUUID } from 'node:crypto';
import type { StorageDriver } from './storage.js';

export class LocalStorageDriver implements StorageDriver {
  private readonly root: string;
  constructor(root: string) { this.root = resolve(root); }

  private async path(key: string, create = false): Promise<string> {
    const parts = key.split('/');
    if (!key || key.includes('\\') || key.includes('\0') || parts.some(part => !part || part === '.' || part === '..')) throw new Error('Unsafe storage key');
    const path = resolve(this.root, ...parts);
    if (!path.startsWith(this.root + sep)) throw new Error('Unsafe storage key');
    if (create) await mkdir(this.root, { recursive: true });
    let current = this.root;
    for (const part of parts.slice(0, -1)) {
      current = resolve(current, part);
      if (create) await mkdir(current).catch((error: NodeJS.ErrnoException) => { if (error.code !== 'EEXIST') throw error; });
      const info = await lstat(current);
      if (!info.isDirectory() || info.isSymbolicLink()) throw new Error('Unsafe storage directory');
    }
    const stat = await lstat(path).catch((error: NodeJS.ErrnoException) => { if (error.code !== 'ENOENT') throw error; return undefined; });
    if (stat && (!stat.isFile() || stat.isSymbolicLink())) throw new Error('Unsafe storage object');
    return path;
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    const path = await this.path(key, true);
    const temp = resolve(dirname(path), `.upload-${randomUUID()}`);
    try { await writeFile(temp, data, { flag: 'wx', mode: 0o600, flush: true }); await rename(temp, path); }
    finally { await rm(temp, { force: true }); }
  }

  async get(key: string): Promise<Uint8Array> { return readFile(await this.path(key)); }
  async delete(key: string): Promise<void> {
    try { await rm(await this.path(key), { force: true }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  }
}
