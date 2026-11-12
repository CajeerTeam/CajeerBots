import { CajeerClient } from '@cajeerbots/sdk';
export function apiClient(): CajeerClient {
  const baseUrl = process.env.CAJEER_API_URL ?? 'http://127.0.0.1:3000';
  const apiKey = process.env.CAJEER_API_KEY ?? process.env.CAJEER_ADMIN_TOKEN;
  const jwt = process.env.CAJEER_JWT;
  if (!apiKey && !jwt) throw new Error('Set CAJEER_API_KEY or CAJEER_JWT (or local CAJEER_ADMIN_TOKEN)');
  const url = new URL(baseUrl);
  if (url.username || url.password || (url.protocol !== 'https:' && !(url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)))) throw new Error('API URL must use HTTPS or loopback HTTP');
  return new CajeerClient({ baseUrl, apiKey, jwt });
}
export function print(value: unknown): void { console.log(JSON.stringify(value, null, 2)); }
