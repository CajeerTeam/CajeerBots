import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { renderVariables } from '@cajeerbots/message-core';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class MessageTemplatesService {
  constructor(private readonly prisma: PrismaService) {}

  list(locale?: string) {
    return this.prisma.messageTemplate.findMany({ where: locale ? { locale } : {}, orderBy: [{ key: 'asc' }, { locale: 'asc' }] });
  }

  async get(key: string, locale = 'ru') {
    const template = await this.prisma.messageTemplate.findUnique({ where: { key_locale: { key, locale } } });
    if (!template) throw new NotFoundException('Message template не найден');
    return template;
  }

  async upsert(input: { key: string; locale: string; body: string; variables?: Record<string, unknown>; buttons?: unknown }, actor: Principal) {
    const data = compact({ body: input.body, variables: input.variables as never, buttons: input.buttons as never });
    const template = await this.prisma.messageTemplate.upsert({
      where: { key_locale: { key: input.key, locale: input.locale } },
      create: { key: input.key, locale: input.locale, ...data },
      update: data,
    });
    await this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'message_template.upsert', resource: 'message_template', resourceId: template.id, metadata: { key: input.key, locale: input.locale } as never } });
    return template;
  }

  async render(key: string, locale: string, variables: Record<string, string | number | boolean | null>) {
    const template = await this.get(key, locale);
    const stringVariables = Object.fromEntries(Object.entries(variables).map(([name, value]) => [name, value === null ? '' : String(value)]));
    return { key, locale, body: renderVariables(template.body, stringVariables), buttons: template.buttons ?? null };
  }
}

function compact<T extends Record<string, unknown>>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined)) as T;
}
