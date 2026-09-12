import { readdir, readFile, stat } from 'node:fs/promises';
import { join, relative, resolve } from 'node:path';

export async function fileExists(path: string): Promise<boolean> {
  try {
    const info = await stat(path);
    return info.isFile();
  } catch {
    return false;
  }
}

export async function directoryExists(path: string): Promise<boolean> {
  try {
    const info = await stat(path);
    return info.isDirectory();
  } catch {
    return false;
  }
}

export async function readTextFile(path: string): Promise<string> {
  return readFile(path, 'utf8');
}

export async function listFilesRecursive(root: string, options: { includeExtensions?: string[]; exclude?: string[]; maxFiles?: number } = {}): Promise<string[]> {
  const output: string[] = [];
  const absoluteRoot = resolve(root);
  const include = new Set(options.includeExtensions ?? []);
  const exclude = options.exclude ?? [];
  const maxFiles = options.maxFiles ?? 5000;

  async function walk(dir: string): Promise<void> {
    if (output.length >= maxFiles) return;
    const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);
    for (const entry of entries) {
      if (output.length >= maxFiles) return;
      const path = join(dir, entry.name);
      const rel = relative(absoluteRoot, path);
      if (exclude.some((item) => rel.includes(item) || entry.name === item)) continue;
      if (entry.isDirectory()) await walk(path);
      else if (entry.isFile()) {
        if (include.size === 0 || [...include].some((ext) => entry.name.endsWith(ext))) output.push(path);
      }
    }
  }

  await walk(absoluteRoot);
  return output;
}
