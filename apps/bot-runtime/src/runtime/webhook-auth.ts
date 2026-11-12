import { createPublicKey, timingSafeEqual, verify } from 'node:crypto';
import type { RuntimeBotAccount } from './load-bot-accounts.js';

export function webhookConfig(account: RuntimeBotAccount): { secret?: string; publicKey?: string; groupId?: string; confirmation?: string } {
  // JSON keyed by exact account ID, provisioned by operator, never by request.
  const configs = JSON.parse(process.env.BOT_WEBHOOK_CREDENTIALS ?? '{}') as Record<string, Record<string, string>>;
  return configs && Object.hasOwn(configs, account.id) ? configs[account.id] ?? {} : {};
}
function equal(a: unknown, b: unknown): boolean {
  if (typeof a !== 'string' || typeof b !== 'string' || !b) return false;
  const x = Buffer.from(a), y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}
export function authenticateWebhook(account: RuntimeBotAccount, headers: Record<string, unknown>, raw: Buffer, payload: Record<string, unknown>, now = Date.now()): boolean {
  const config = webhookConfig(account);
  if (account.provider === 'telegram') return equal(headers['x-telegram-bot-api-secret-token'], config.secret);
  if (account.provider === 'vk') return equal(payload.secret, config.secret) && equal(String(payload.group_id ?? ''), config.groupId);
  const signature = headers['x-signature-ed25519'];
  const timestamp = headers['x-signature-timestamp'];
  if (typeof timestamp !== 'string' || !/^\d{10}$/.test(timestamp) || Math.abs(now - Number(timestamp) * 1000) > 300_000 || typeof signature !== 'string' || !/^[a-f0-9]{128}$/i.test(signature) || !config.publicKey || !/^[a-f0-9]{64}$/i.test(config.publicKey)) return false;
  try {
    const key = createPublicKey({ key: Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), Buffer.from(config.publicKey, 'hex')]), format: 'der', type: 'spki' });
    return verify(null, Buffer.concat([Buffer.from(timestamp), raw]), key, Buffer.from(signature, 'hex'));
  } catch { return false; }
}
