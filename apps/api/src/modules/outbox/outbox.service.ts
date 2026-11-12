import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
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
    const message = await this.prisma.$transaction(async (tx) => {
      const changed = await tx.outboxMessage.updateMany({ where: { id, status: { in: ['failed', 'dead'] } }, data: { status: 'pending', availableAt: new Date(), attempts: 0 } });
      if (!changed.count) throw new ConflictException('Only failed or dead messages can be retried');
      await tx.webhookDelivery.updateMany({ where: { outboxMessageId: id, status: { in: ['failed', 'dead'] } }, data: { status: 'pending', attempts: 0, availableAt: new Date(), lastError: null } });
      return tx.outboxMessage.findUniqueOrThrow({ where: { id } });
    });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'outbox.retry', resource: 'outbox_message', resourceId: id } });
    return message;
  }
}
