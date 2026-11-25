import { BadRequestException, ConflictException, ForbiddenException, Injectable, NotFoundException } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { PrismaService, workspaceTarget, workspaceKeyAllowed, workspaceMarker, workspaceText, workspacePrefix } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class BotRuntimeService {
  constructor(private readonly prisma: PrismaService) {}


  private target(communityId: string, principal: Principal) {
    const target = principal.type === 'api_key' && workspaceKeyAllowed(principal.scopes, communityId) ? workspaceTarget(principal.id, communityId) : null;
    if (!target) throw new ForbiddenException('Workspace target not allowed');
    return target;
  }

  async workspaceTarget(communityId: string, principal: Principal) {
    const target = this.target(communityId, principal);
    const account = await this.prisma.botAccount.findFirst({ where: { id: target.accountId, communityId, provider: target.provider, isActive: true } });
    if (!account || !account.tokenRef) throw new ConflictException('Workspace account unavailable');
    return { ...target, revision: account.updatedAt.toISOString(), api: 'workspace-delivery-v1', credentialId: principal.id };
  }

  private workspaceId(id: string) {
    if (!/^workspace-[a-f0-9]{32}$/.test(id)) throw new BadRequestException('Invalid Workspace message ID');
  }

  private async workspaceRow(communityId: string, id: string, principal: Principal) {
    this.workspaceId(id);
    const target = this.target(communityId, principal);
    const row = await this.prisma.botOutboundMessage.findFirst({ where: { id, communityId, accountId: target.accountId, provider: target.provider, targetId: target.channelId, targetType: { startsWith: `workspace:${principal.id}:` } } });
    if (!row) throw new NotFoundException('Workspace message not found');
    return row;
  }

  private workspaceReceipt(row: Awaited<ReturnType<BotRuntimeService['workspaceRow']>>) {
    const status = row.status === 'dead' ? (row.lastError === 'workspace_preflight' ? 'retryable' : row.lastError === 'workspace_cancelled' ? 'cancelled' : 'uncertain') : row.status === 'sending' && row.availableAt.getTime() <= Date.now() ? 'uncertain' : row.status === 'failed' ? 'uncertain' : row.status;
    return { id: row.id, communityId: row.communityId, accountId: row.accountId, provider: row.provider, channelId: row.targetId, revision: row.targetType.substring(row.targetType.indexOf(':', 'workspace:'.length) + 1), sha256: createHash('sha256').update(row.text, 'utf8').digest('hex'), status, attempts: row.attempts, providerMessageId: row.providerMessageId };
  }

  async workspaceMessage(communityId: string, id: string, principal: Principal) {
    return this.workspaceReceipt(await this.workspaceRow(communityId, id, principal));
  }

  async workspaceQueue(communityId: string, id: string, input: unknown, principal: Principal) {
    this.workspaceId(id);
    if (!input || typeof input !== 'object' || Array.isArray(input)) throw new BadRequestException('Invalid Workspace payload');
    const body = input as Record<string, unknown>;
    if (Object.keys(body).sort().join(',') !== 'accountId,channelId,revision,text' || !workspaceText(body.text)) throw new BadRequestException('Invalid Workspace payload');
    const target = await this.workspaceTarget(communityId, principal);
    if (body.accountId !== target.accountId || body.channelId !== target.channelId || body.revision !== target.revision) throw new ConflictException('Workspace target changed');
    const text = body.text;
    const marker = workspaceMarker(principal.id, target.revision);
    // An immutable primary key is the remote idempotency boundary. No update of existing content.
    const row = await this.prisma.botOutboundMessage.upsert({ where: { id }, create: { id, communityId, accountId: target.accountId, provider: target.provider, targetId: target.channelId, targetType: marker, text }, update: {} });
    if (row.communityId !== communityId || row.accountId !== target.accountId || row.provider !== target.provider || row.targetId !== target.channelId || row.targetType !== marker || row.text !== text) throw new ConflictException('Workspace message ID already used');
    return this.workspaceReceipt(row);
  }

  async workspaceChange(communityId: string, id: string, operation: string, principal: Principal) {
    const row = await this.workspaceRow(communityId, id, principal);
    if (operation === 'cancel') {
      if (row.status === 'dead' && row.lastError === 'workspace_cancelled') return this.workspaceReceipt(row);
      const changed = await this.prisma.botOutboundMessage.updateMany({ where: { id, targetType: row.targetType, OR: [{ status: 'pending' }, { status: 'dead', lastError: 'workspace_preflight' }] }, data: { status: 'dead', lastError: 'workspace_cancelled' } });
      if (!changed.count) throw new ConflictException('Provider delivery already claimed');
    } else if (operation === 'retry') {
      const target = await this.workspaceTarget(communityId, principal);
      if (row.targetType !== workspaceMarker(principal.id, target.revision)) throw new ConflictException('Workspace target changed');
      const changed = await this.prisma.botOutboundMessage.updateMany({ where: { id, targetType: row.targetType, status: 'dead', lastError: 'workspace_preflight', attempts: { lt: 8 } }, data: { status: 'pending', availableAt: new Date(), lastError: null } });
      if (!changed.count) throw new ConflictException('Only verified pre-send failure can be retried');
    } else throw new BadRequestException('Invalid Workspace operation');
    return this.workspaceMessage(communityId, id, principal);
  }

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
      where: { id, communityId, NOT: { id: { startsWith: workspacePrefix } }, provider: { not: 'discord' }, status: { in: ['failed', 'dead'] } },
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
