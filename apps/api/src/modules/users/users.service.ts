import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';
import type { BotProvider } from '@cajeerbots/core';

@Injectable()
export class UsersService {
  constructor(private readonly prisma: PrismaService) {}

  list(communityId: string) {
    return this.prisma.user.findMany({
      where: { communityId },
      orderBy: { createdAt: 'desc' },
      include: { roles: { include: { role: { include: { permissions: { include: { permission: true } } } } } } },
    });
  }

  async get(communityId: string, id: string) {
    const user = await this.prisma.user.findFirst({
      where: { id, communityId },
      include: { roles: { include: { role: { include: { permissions: { include: { permission: true } } } } } } },
    });
    if (!user) throw new NotFoundException('Пользователь не найден');
    return user;
  }

  async create(communityId: string, data: { provider: BotProvider; externalId: string; username?: string; displayName?: string; locale: string }, actor: Principal) {
    try {
      const user = await this.prisma.user.create({ data: { communityId, ...defined(data) } });
      await this.audit(actor, 'user.create', user.id, { communityId, provider: data.provider, externalId: data.externalId });
      return user;
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Пользователь с таким provider/externalId уже существует');
      throw error;
    }
  }

  async update(communityId: string, id: string, data: Partial<{ username: string; displayName: string; locale: string }>, actor: Principal) {
    await this.get(communityId, id);
    const user = await this.prisma.user.update({ where: { id }, data: defined(data) });
    await this.audit(actor, 'user.update', id, { communityId, fields: Object.keys(data) });
    return user;
  }

  async assignRole(communityId: string, userId: string, input: { roleId?: string; roleCode?: string }, actor: Principal) {
    await this.get(communityId, userId);
    const role = input.roleId
      ? await this.prisma.role.findFirst({ where: { id: input.roleId, communityId } })
      : await this.prisma.role.findUnique({ where: { communityId_code: { communityId, code: input.roleCode ?? '' } } });
    if (!role) throw new NotFoundException('Роль не найдена');
    const relation = await this.prisma.userRole.upsert({
      where: { userId_roleId: { userId, roleId: role.id } },
      create: { userId, roleId: role.id },
      update: {},
    });
    await this.audit(actor, 'user.role.assign', userId, { communityId, roleId: role.id, roleCode: role.code });
    return relation;
  }

  async removeRole(communityId: string, userId: string, roleId: string, actor: Principal) {
    await this.get(communityId, userId);
    const role = await this.prisma.role.findFirst({ where: { id: roleId, communityId } });
    if (!role) throw new NotFoundException('Роль не найдена');
    await this.prisma.userRole.delete({ where: { userId_roleId: { userId, roleId } } });
    await this.audit(actor, 'user.role.remove', userId, { communityId, roleId });
    return { removed: true, userId, roleId };
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'user', resourceId, metadata: metadata as never } });
  }
}

function defined<T extends Record<string, unknown>>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined)) as T;
}

function isUniqueError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === 'P2002';
}
