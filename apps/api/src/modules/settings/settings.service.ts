import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class SettingsService {
  constructor(private readonly prisma: PrismaService) {}

  list(communityId: string) {
    return this.prisma.setting.findMany({ where: { communityId }, orderBy: { key: 'asc' } });
  }

  async get(communityId: string, key: string) {
    const setting = await this.prisma.setting.findUnique({ where: { communityId_key: { communityId, key } } });
    if (!setting) throw new NotFoundException('Настройка не найдена');
    return setting;
  }

  history(communityId: string, key: string) {
    return this.prisma.settingHistory.findMany({
      where: { setting: { communityId, key } },
      orderBy: { createdAt: 'desc' },
      take: 100,
    });
  }

  async upsert(communityId: string, key: string, value: unknown, actor: Principal, reason?: string) {
    const existing = await this.prisma.setting.findUnique({ where: { communityId_key: { communityId, key } } });
    const setting = await this.prisma.setting.upsert({
      where: { communityId_key: { communityId, key } },
      create: { communityId, key, value: value as never },
      update: { value: value as never },
    });
    await this.prisma.settingHistory.create({
      data: {
        settingId: setting.id,
        oldValue: existing?.value as never,
        newValue: value as never,
        actorId: actor.id,
        reason,
      },
    });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'setting.upsert', resource: 'setting', resourceId: setting.id, metadata: { communityId, key, reason } as never } });
    return setting;
  }
}
