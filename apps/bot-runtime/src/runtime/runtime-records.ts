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
      version: process.env.npm_package_version ?? '1.0.0-rc.2',
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

export async function recordInboundUpdate(prisma: PrismaService, account: RuntimeBotAccount, input: InboundRecordInput): Promise<{ id: string; isDuplicate: boolean }> {
  if (account.id.startsWith('env:')) return { id: '', isDuplicate: false };
  if (input.providerUpdateId) {
    const existing = await prisma.botInboundUpdate.findUnique({
      where: { provider_accountId_providerUpdateId: { provider: account.provider, accountId: account.id, providerUpdateId: input.providerUpdateId } },
      select: { id: true },
    });
    if (existing) return { id: existing.id, isDuplicate: true };
  }

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
    },
    select: { id: true },
  });
  return { id: created.id, isDuplicate: false };
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
  const created = await prisma.botOutboundMessage.create({
    data: {
      accountId: account.id,
      communityId: account.communityId,
      inboundUpdateId: input.inboundUpdateId,
      provider: account.provider,
      targetType: input.targetType ?? 'chat',
      targetId: input.targetId,
      text: input.text,
      buttons: input.buttons as never,
    },
    select: { id: true, text: true, targetId: true },
  });
  return created;
}

export async function markOutboundSent(prisma: PrismaService, id: string, providerMessageId?: string): Promise<void> {
  if (!id) return;
  await prisma.botOutboundMessage.update({ where: { id }, data: { status: 'sent', attempts: { increment: 1 }, providerMessageId, sentAt: new Date(), lastError: null } });
}

export async function markOutboundFailed(prisma: PrismaService, id: string, error: unknown): Promise<void> {
  if (!id) return;
  const message = error instanceof Error ? error.message : String(error);
  await prisma.botOutboundMessage.update({
    where: { id },
    data: { status: 'failed', attempts: { increment: 1 }, lastError: message, availableAt: new Date(Date.now() + 60_000) },
  });
}
