import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class CommunitiesService {
  constructor(private readonly prisma: PrismaService) {}

  list() {
    return this.prisma.community.findMany({ orderBy: { createdAt: 'desc' } });
  }

  async create(data: { slug: string; title: string; language: string }, actor: Principal) {
    try {
      return await this.prisma.$transaction(async (tx) => {
        const community = await tx.community.create({ data });
        await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'community.create', resource: 'community', resourceId: community.id, metadata: data as never } });
        return community;
      });
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException(`Community со slug ${data.slug} уже существует`);
      throw error;
    }
  }

  async get(id: string) {
    const community = await this.prisma.community.findUnique({ where: { id } });
    if (!community) throw new NotFoundException('Community не найдена');
    return community;
  }

  async update(id: string, data: Partial<{ slug: string; title: string; language: string }>, actor: Principal) {
    try {
      return await this.prisma.$transaction(async (tx) => {
        const community = await tx.community.update({ where: { id }, data });
        await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'community.update', resource: 'community', resourceId: id, metadata: data as never } });
        return community;
      });
    } catch (error) {
      if (isPrismaError(error, 'P2025')) throw new NotFoundException('Community не найдена');
      if (isUniqueError(error)) throw new ConflictException(`Community со slug ${data.slug} уже существует`);
      throw error;
    }
  }
}

function isUniqueError(error: unknown): boolean {
  return isPrismaError(error, 'P2002');
}

function isPrismaError(error: unknown, code: string): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === code;
}
