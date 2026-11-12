import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class BotRuntimeService {
  constructor(private readonly prisma: PrismaService) {}

  async status(communityId: string, accountId?: string) {
    const where = { account: { communityId }, ...(accountId ? { accountId } : {}) };
    const [accounts, statuses] = await Promise.all([
      this.prisma.botAccount.findMany({ where: { communityId }, orderBy: { createdAt: 'asc' } }),
      this.prisma.botRuntimeStatus.findMany({ where, orderBy: { lastBeatAt: 'desc' }, take: 200 }),
    ]);
    const latest = new Map<string, (typeof statuses)[number]>();
    for (const status of statuses) {
      if (!latest.has(status.accountId)) latest.set(status.accountId, status);
    }
    return accounts.map((account) => {
      const status = latest.get(account.id);
      return {
        accountId: account.id,
        provider: account.provider,
        mode: account.mode,
        username: account.username,
        isActive: account.isActive,
        runtime: status ? {
          id: status.id,
          status: status.status,
          version: status.version,
          hostname: status.hostname,
          pid: status.pid,
          startedAt: status.startedAt,
          lastBeatAt: status.lastBeatAt,
          stoppedAt: status.stoppedAt,
          error: status.error,
          stale: Date.now() - status.lastBeatAt.getTime() > Number(process.env.BOT_RUNTIME_STALE_MS ?? 60_000),
        } : null,
      };
    });
  }

  listInbound(communityId: string, query: { accountId?: string; status?: string; take: number }) {
    return this.prisma.botInboundUpdate.findMany({
      where: { communityId, ...(query.accountId ? { accountId: query.accountId } : {}), ...(query.status ? { status: query.status } : {}) },
      orderBy: { receivedAt: 'desc' },
      take: query.take,
    });
  }

  listOutbound(communityId: string, query: { accountId?: string; status?: string; take: number }) {
    return this.prisma.botOutboundMessage.findMany({
      where: { communityId, ...(query.accountId ? { accountId: query.accountId } : {}), ...(query.status ? { status: query.status as never } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
    });
  }

  async retryOutbound(communityId: string, id: string, actor: Principal, resetAttempts = false) {
    const message = await this.prisma.botOutboundMessage.findFirst({ where: { id, communityId } });
    if (!message) throw new NotFoundException('Outbound message не найден');
    const changed = await this.prisma.botOutboundMessage.updateMany({
      where: { id, communityId, provider: { not: 'discord' }, status: { in: ['failed', 'dead'] } },
      data: { status: 'pending', availableAt: new Date(), lastError: null, ...(resetAttempts ? { attempts: 0 } : {}) },
    });
    if (!changed.count) throw new ConflictException('Only failed non-interaction messages can be retried');
    const updated = await this.prisma.botOutboundMessage.findUniqueOrThrow({ where: { id } });
    await this.prisma.auditLog.create({
      data: { actorId: actor.id, actorType: actor.type, action: 'bot_runtime.outbound.retry', resource: 'bot_outbound_message', resourceId: id, metadata: { communityId, resetAttempts } as never },
    });
    return updated;
  }

  async webhookUrl(communityId: string, accountId: string) {
    const account = await this.prisma.botAccount.findFirst({ where: { id: accountId, communityId } });
    if (!account) throw new NotFoundException('Bot account не найден');
    const base = process.env.BOT_WEBHOOK_PUBLIC_URL ?? process.env.APP_PUBLIC_URL;
    const path = `/runtime/webhooks/${account.provider}/${account.id}`;
    return { accountId: account.id, provider: account.provider, mode: account.mode, url: base ? new URL(path, base).toString() : path };
  }
}
