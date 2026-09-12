import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class OutboxService {
  constructor(private readonly prisma: PrismaService) {}

  list(status?: string, take = 50) {
    return this.prisma.outboxMessage.findMany({
      where: status ? { status: status as never } : {},
      orderBy: { createdAt: 'desc' },
      take: Math.min(Math.max(take, 1), 200),
      include: { deliveries: { include: { endpoint: true } } },
    });
  }

  async get(id: string) {
    const message = await this.prisma.outboxMessage.findUnique({ where: { id }, include: { deliveries: { include: { endpoint: true } } } });
    if (!message) throw new NotFoundException('Outbox message не найден');
    return message;
  }

  async retry(id: string, actor: Principal) {
    await this.get(id);
    await this.prisma.webhookDelivery.updateMany({ where: { outboxMessageId: id, status: { in: ['failed', 'dead'] } }, data: { status: 'pending', availableAt: new Date(), lastError: null } });
    const message = await this.prisma.outboxMessage.update({ where: { id }, data: { status: 'pending', availableAt: new Date() } });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'outbox.retry', resource: 'outbox_message', resourceId: id } });
    return message;
  }
}
