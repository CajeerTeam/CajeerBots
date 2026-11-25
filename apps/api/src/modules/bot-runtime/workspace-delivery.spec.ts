import { afterEach, describe, expect, it, vi } from 'vitest';
import type { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';
import { BotRuntimeService } from './bot-runtime.service.js';
const principal: Principal = { id: 'key', type: 'api_key', scopes: ['workspace:messages', 'community:c1'] };
const revision = '2026-11-25T00:00:00.000Z';
const id = 'workspace-' + 'a'.repeat(32);
const input = { accountId: 'a1', channelId: '-100123456789', revision, text: 'Release v1' };
afterEach(() => vi.unstubAllEnvs());
describe('Workspace outbound API', () => {
  it('idempotent immutable message, community, target, cancel and unknown-state fences', async () => {
    vi.stubEnv('WORKSPACE_BOT_TARGETS', JSON.stringify({ key: { communityId: 'c1', accountId: 'a1', provider: 'telegram', channelId: '-100123456789' } }));
    let row: any;
    const prisma = { botAccount: { findFirst: vi.fn(async () => ({ updatedAt: new Date(revision), tokenRef: 'env:TEST' })) }, botOutboundMessage: {
      upsert: vi.fn(async ({ create }: { create: Record<string, unknown> }) => row ??= { ...create, status: 'pending', attempts: 0, providerMessageId: null, availableAt: new Date(), lastError: null }),
      findFirst: vi.fn(async ({ where }: { where: { id: string } }) => row && where.id === row.id ? row : null),
      updateMany: vi.fn(async ({ data }: { data: Record<string, unknown> }) => { if (row.status !== 'pending') return { count: 0 }; row = { ...row, ...data }; return { count: 1 }; }),
    }} as unknown as PrismaService;
    const api = new BotRuntimeService(prisma);
    await expect(api.workspaceTarget('c2', principal)).rejects.toThrow();
    await expect(api.workspaceQueue('c1', id, { ...input, channelId: '-100999999999' }, principal)).rejects.toThrow();
    const receipt = await api.workspaceQueue('c1', id, input, principal);
    expect(receipt.status).toBe('pending'); expect(receipt.revision).toBe(revision);
    expect(await api.workspaceQueue('c1', id, input, principal)).toEqual(receipt);
    await expect(api.workspaceQueue('c1', id, { ...input, text: 'changed' }, principal)).rejects.toThrow();
    row.status = 'sending'; row.availableAt = new Date(0);
    expect((await api.workspaceMessage('c1', id, principal)).status).toBe('uncertain');
    await expect(api.workspaceChange('c1', id, 'retry', principal)).rejects.toThrow();
    row.status = 'pending';
    expect((await api.workspaceChange('c1', id, 'cancel', principal)).status).toBe('cancelled');
    expect((await api.workspaceChange('c1', id, 'cancel', principal)).status).toBe('cancelled');
  });
});
