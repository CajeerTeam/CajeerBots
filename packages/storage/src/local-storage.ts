import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import type { StorageDriver } from './storage.js';

export class LocalStorageDriver implements StorageDriver {
  constructor(private readonly root: string) {}

  async put(key: string, data: Uint8Array): Promise<void> {
    const path = join(this.root, key);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, data);
  }

  async get(key: string): Promise<Uint8Array> {
    return readFile(join(this.root, key));
  }

  async delete(key: string): Promise<void> {
    await rm(join(this.root, key), { force: true });
  }
}
