import type { PrismaService } from '@cajeerbots/db';
import type { RuntimeBotAccount } from './load-bot-accounts.js';

export interface RuntimeStatusHandle {
  id: string;
  beat(): Promise<void>;
  stop(error?: unknown): Promise<void>;
}

export async function startRuntimeStatus(prisma: PrismaService, account: RuntimeBotAccount): Promise<RuntimeStatusHandle> {
  if (account.id.startsWith('env:')) {
    return { id: account.id, async beat() {}, async stop() {} };
  }
  const row = await prisma.botRuntimeStatus.create({
    data: {
      accountId: account.id,
      provider: account.provider,
      mode: account.mode,
      status: 'running',
      pid: process.pid,
      hostname: process.env.HOSTNAME,
      version: process.env.npm_package_version ?? '1.0.0-rc.3',
      metadata: { username: account.username ?? null, mode: account.mode } as never,
    },
  });

  return {
    id: row.id,
    async beat() {
      await prisma.botRuntimeStatus.update({ where: { id: row.id }, data: { status: 'running', lastBeatAt: new Date(), error: null } });
    },
    async stop(error?: unknown) {
      const message = error instanceof Error ? error.message : error ? String(error) : null;
      await prisma.botRuntimeStatus.update({ where: { id: row.id }, data: { status: message ? 'failed' : 'stopped', stoppedAt: new Date(), error: message } });
    },
  };
}

export interface InboundRecordInput {
  providerUpdateId?: string;
  type: string;
  userExternalId?: string;
  chatExternalId?: string;
  payload: Record<string, unknown>;
}

export async function recordInboundUpdate(prisma: PrismaService, account: RuntimeBotAccount, input: InboundRecordInput): Promise<{ id: string; isDuplicate: boolean; status?: string }> {
  if (account.id.startsWith('env:')) return { id: '', isDuplicate: false };
  try {
  const created = await prisma.botInboundUpdate.create({
    data: {
      accountId: account.id,
      communityId: account.communityId,
      provider: account.provider,
      providerUpdateId: input.providerUpdateId,
      type: input.type,
      userExternalId: input.userExternalId,
      chatExternalId: input.chatExternalId,
      payload: input.payload as never,
      status: 'processing',
    },
    select: { id: true },
  });
  return { id: created.id, isDuplicate: false };
  } catch (error) {
    if (!input.providerUpdateId || (error as { code?: string }).code !== 'P2002') throw error;
    const existing = await prisma.botInboundUpdate.findUniqueOrThrow({ where: { provider_accountId_providerUpdateId: { provider: account.provider, accountId: account.id, providerUpdateId: input.providerUpdateId } } });
    const claimed = await prisma.botInboundUpdate.updateMany({ where: { id: existing.id, OR: [{ status: 'failed' }, { status: { in: ['received', 'processing'] }, receivedAt: { lt: new Date(Date.now() - 300_000) } }] }, data: { status: 'processing', receivedAt: new Date(), error: null } });
    return { id: existing.id, isDuplicate: !claimed.count, status: claimed.count ? 'processing' : existing.status };
  }
}

export async function markInboundHandled(prisma: PrismaService, id: string, status: 'handled' | 'ignored' | 'failed', error?: unknown): Promise<void> {
  if (!id) return;
  const message = error instanceof Error ? error.message : error ? String(error) : null;
  await prisma.botInboundUpdate.update({ where: { id }, data: { status, handledAt: new Date(), error: message } });
}

export async function createOutboundMessage(
  prisma: PrismaService,
  account: RuntimeBotAccount,
  input: { inboundUpdateId?: string; targetId: string; targetType?: string; text: string; buttons?: unknown },
): Promise<{ id: string; text: string; targetId: string }> {
  if (account.id.startsWith('env:')) return { id: '', text: input.text, targetId: input.targetId };
  const data = {
      accountId: account.id,
      communityId: account.communityId,
      inboundUpdateId: input.inboundUpdateId,
      provider: account.provider,
      targetType: input.targetType ?? 'chat',
      targetId: input.targetId,
      text: input.text,
      buttons: input.buttons as never,
  };
  const created = input.inboundUpdateId
    ? await prisma.botOutboundMessage.upsert({ where: { id: `inbound-${input.inboundUpdateId}` }, create: { ...data, id: `inbound-${input.inboundUpdateId}` }, update: {}, select: { id: true, text: true, targetId: true } })
    : await prisma.botOutboundMessage.create({ data, select: { id: true, text: true, targetId: true } });
  return created;
}

export async function markOutboundSent(prisma: PrismaService, id: string, providerMessageId?: string): Promise<void> {
  if (!id) return;
  await prisma.botOutboundMessage.update({ where: { id }, data: { status: 'sent', attempts: { increment: 1 }, providerMessageId, sentAt: new Date(), lastError: null } });
}

export async function markOutboundFailed(prisma: PrismaService, id: string, error: unknown): Promise<void> {
  if (!id) return;
  const message = error instanceof Error ? error.name : 'DeliveryError';
  await prisma.botOutboundMessage.update({
    where: { id },
    data: { status: 'failed', attempts: { increment: 1 }, lastError: message, availableAt: new Date(Date.now() + 60_000) },
  });
}
