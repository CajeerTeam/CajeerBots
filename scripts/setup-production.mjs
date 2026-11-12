import { randomBytes } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';

const url = new URL(process.argv[2] ?? 'https://bots.example.org');
if (url.protocol !== 'https:' || url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
  throw new Error('Укажите HTTPS origin, например https://bots.example.org');
}
const template = await readFile(new URL('../.env.production.example', import.meta.url), 'utf8');
const password = randomBytes(32).toString('base64url');
const text = template.replaceAll('https://bots.example.org', url.origin)
  .replaceAll('replace-with-generated-password', password)
  .replaceAll('replace-with-generated-secret', () => randomBytes(48).toString('base64url'));
await writeFile(new URL('../.env.production', import.meta.url), text, { flag: 'wx', mode: 0o600 });
console.log('Создан .env.production (0600). Существующие конфигурации не перезаписываются.');
