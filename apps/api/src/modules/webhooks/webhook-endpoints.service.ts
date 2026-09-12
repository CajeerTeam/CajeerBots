import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class WebhookEndpointsService {
  constructor(private readonly prisma: PrismaService) {}

  list() {
    return this.prisma.webhookEndpoint.findMany({ orderBy: { createdAt: 'desc' }, include: { deliveries: { orderBy: { createdAt: 'desc' }, take: 5 } } });
  }

  async create(data: { title: string; url: string; events: string[]; secret?: string; isActive: boolean }, actor: Principal) {
    const endpoint = await this.prisma.webhookEndpoint.create({
      data: { title: data.title, url: data.url, events: data.events, secretRef: data.secret, isActive: data.isActive },
    });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'webhook_endpoint.create', resource: 'webhook_endpoint', resourceId: endpoint.id, metadata: { title: data.title, url: data.url, events: data.events } as never } });
    return endpoint;
  }

  async setActive(id: string, isActive: boolean, actor: Principal) {
    const existing = await this.prisma.webhookEndpoint.findUnique({ where: { id } });
    if (!existing) throw new NotFoundException('Webhook endpoint не найден');
    const endpoint = await this.prisma.webhookEndpoint.update({ where: { id }, data: { isActive } });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: isActive ? 'webhook_endpoint.enable' : 'webhook_endpoint.disable', resource: 'webhook_endpoint', resourceId: id } });
    return endpoint;
  }
}
