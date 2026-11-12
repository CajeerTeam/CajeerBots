import { BadRequestException, ConflictException, ForbiddenException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { validBotTokenRef, canAccessCommunity, type Principal } from '@cajeerbots/security';
import type { BotMode, BotProvider } from '@cajeerbots/core';

@Injectable()
export class BotAccountsService {
  constructor(private readonly prisma: PrismaService) {}

  list(communityId: string) {
    return this.prisma.botAccount.findMany({ where: { communityId }, orderBy: { createdAt: 'desc' }, omit: { tokenRef: true } });
  }

  async get(communityId: string, id: string) {
    const account = await this.prisma.botAccount.findFirst({ where: { id, communityId }, omit: { tokenRef: true } });
    if (!account) throw new NotFoundException('Bot account не найден');
    return account;
  }

  async create(
    communityId: string,
    data: { provider: BotProvider; mode: BotMode; externalId?: string; username?: string; tokenRef?: string; isActive: boolean },
    actor: Principal,
  ) {
    if (!canAccessCommunity(actor)) throw new ForbiddenException('Настройка bot credentials доступна только platform admin');
    if (data.tokenRef && !validBotTokenRef(data.provider, data.tokenRef, communityId)) throw new BadRequestException('Invalid provider token reference');
    try {
      const account = await this.prisma.botAccount.create({ data: { communityId, ...defined(data) }, omit: { tokenRef: true } });
      await this.audit(actor, 'bot_account.create', account.id, { communityId, provider: data.provider, mode: data.mode });
      return account;
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Bot account с таким community/provider/externalId уже существует');
      throw error;
    }
  }

  async update(
    communityId: string,
    id: string,
    data: Partial<{ mode: BotMode; externalId: string; username: string; tokenRef: string; isActive: boolean }>,
    actor: Principal,
  ) {
    if (!canAccessCommunity(actor)) throw new ForbiddenException('Изменение bot accounts доступно только platform admin');
    const existing = await this.get(communityId, id);
    if (data.tokenRef && !validBotTokenRef(existing.provider, data.tokenRef, communityId)) throw new BadRequestException('Invalid provider token reference');
    const account = await this.prisma.botAccount.update({ where: { id }, data: defined(data), omit: { tokenRef: true } });
    await this.audit(actor, 'bot_account.update', id, { communityId, fields: Object.keys(data) });
    return account;
  }

  async setActive(communityId: string, id: string, isActive: boolean, actor: Principal) {
    return this.update(communityId, id, { isActive }, actor);
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'bot_account', resourceId, metadata: metadata as never } });
  }
}

function defined<T extends Record<string, unknown>>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined)) as T;
}

function isUniqueError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === 'P2002';
}
