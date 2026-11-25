import { afterEach, describe, expect, it, vi } from 'vitest';
import type { PrismaService } from '@cajeerbots/db';
import { BotOutboundWorker } from './bot-outbound.worker.js';
const revision = '2026-11-25T00:00:00.000Z';
afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); });
describe('Workspace delivery worker', () => {
  it('never repeats an ambiguous provider request and preserves receipt on success', async () => {
    vi.stubEnv('WORKSPACE_BOT_TARGETS', JSON.stringify({ key: { communityId: 'c1', accountId: 'a1', provider: 'telegram', channelId: '-100123456789' } }));
    vi.stubEnv('TELEGRAM_BOT_TOKEN_WORKSPACE', 'test-only');
    const row = { id: 'workspace-' + 'a'.repeat(32), communityId: 'c1', accountId: 'a1', provider: 'telegram', targetId: '-100123456789', targetType: `workspace:key:${revision}`, text: 'Release', status: 'pending', attempts: 0, lastError: null as string | null, providerMessageId: null as string | null, availableAt: new Date(0), account: { id: 'a1', communityId: 'c1', provider: 'telegram', isActive: true, tokenRef: 'env:TELEGRAM_BOT_TOKEN_WORKSPACE', updatedAt: new Date(revision) } };
    const prisma = { botOutboundMessage: { findMany: vi.fn(async () => row.status === 'pending' ? [row] : []), findUniqueOrThrow: vi.fn(async () => row), updateMany: vi.fn(async ({ where, data }: { where: { status: string }; data: Record<string, unknown> & { attempts?: { increment: number } } }) => {
      if (where.status !== row.status) return { count: 0 }; const { attempts, ...rest } = data; Object.assign(row, rest); if (attempts?.increment) row.attempts += attempts.increment; return { count: 1 };
    }) }, apiKey: { findFirst: vi.fn(async () => ({ scopes: ['workspace:messages', 'community:c1'] })) } } as unknown as PrismaService;
    const worker = new BotOutboundWorker(prisma);
    const send = vi.fn(async () => { throw new Error('lost response'); }); vi.stubGlobal('fetch', send);
    await worker.tick(); expect(row.status).toBe('dead'); expect(row.lastError).toBe('workspace_uncertain'); await worker.tick(); expect(send).toHaveBeenCalledTimes(1);
    // Explicitly simulate a separate queued message and an actual pre-send failure.
    row.id = 'workspace-' + 'b'.repeat(32); row.status = 'pending'; row.attempts = 0; row.availableAt = new Date(0); row.account.isActive = false;
    await worker.tick(); expect(row.lastError).toBe('workspace_preflight'); expect(send).toHaveBeenCalledTimes(1);
    // Only a safe pre-send failure is retried by the dedicated API.
    row.status = 'pending'; row.availableAt = new Date(0); row.account.isActive = true;
    const successful = vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ ok: true, result: { message_id: 123 } }) })); vi.stubGlobal('fetch', successful);
    await worker.tick(); expect(row.status).toBe('sent'); expect(row.providerMessageId).toBe('123'); expect(successful).toHaveBeenCalledTimes(1);
  });
});
