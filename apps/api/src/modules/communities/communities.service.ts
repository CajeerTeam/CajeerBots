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
      const community = await this.prisma.community.create({ data });
      await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'community.create', resource: 'community', resourceId: community.id, metadata: data as never } });
      return community;
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
    await this.get(id);
    const community = await this.prisma.community.update({ where: { id }, data });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'community.update', resource: 'community', resourceId: id, metadata: data as never } });
    return community;
  }
}

function isUniqueError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === 'P2002';
}
