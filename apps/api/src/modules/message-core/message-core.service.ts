import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService, findMessageButtonLayout, upsertMessageButtonLayout } from '@cajeerbots/db';
import { renderStructuredMessage, validateBotForm, type BotFormField, type MessageProvider } from '@cajeerbots/message-core';
import type { Principal } from '@cajeerbots/security';

interface RenderInput {
  locale: string;
  provider?: MessageProvider;
  templateKey?: string;
  catalogKey?: string;
  namespace: string;
  text?: string;
  variables: Record<string, unknown>;
  buttonLayoutCode?: string;
  buttons?: unknown;
  parseMode: 'plain' | 'markdown' | 'html';
  missingVariables: 'empty' | 'keep' | 'throw';
}

@Injectable()
export class MessageCoreService {
  constructor(private readonly prisma: PrismaService) {}

  catalogs(communityId: string, locale?: string, namespace?: string) {
    return this.prisma.localeCatalog.findMany({
      where: { communityId, ...(locale ? { locale } : {}), ...(namespace ? { namespace } : {}) },
      orderBy: [{ namespace: 'asc' }, { locale: 'asc' }],
    });
  }

  async upsertCatalog(communityId: string, input: { namespace: string; locale: string; fallbackLocale?: string | null; messages: Record<string, string> }, actor: Principal) {
    const catalog = await this.prisma.localeCatalog.upsert({
      where: { communityId_namespace_locale: { communityId, namespace: input.namespace, locale: input.locale } },
      create: { communityId, namespace: input.namespace, locale: input.locale, fallbackLocale: input.fallbackLocale ?? null, messages: input.messages as never },
      update: { fallbackLocale: input.fallbackLocale ?? null, messages: input.messages as never, version: { increment: 1 } },
    });
    await this.audit(actor, 'message_core.catalog.upsert', 'locale_catalog', catalog.id, { communityId, namespace: input.namespace, locale: input.locale });
    return catalog;
  }

  buttonLayouts(communityId: string, code?: string, provider?: MessageProvider, locale?: string) {
    return this.prisma.messageButtonLayout.findMany({
      where: { communityId, ...(code ? { code } : {}), ...(provider ? { provider } : {}), ...(locale ? { locale } : {}) },
      orderBy: [{ code: 'asc' }, { locale: 'asc' }],
    });
  }

  async upsertButtonLayout(communityId: string, input: { code: string; provider?: MessageProvider | null; locale: string; rows: unknown }, actor: Principal) {
    const row = await this.prisma.$transaction((tx) => upsertMessageButtonLayout(tx,
      { communityId, code: input.code, provider: input.provider ?? null, locale: input.locale, rows: input.rows as never },
      { rows: input.rows as never },
    ));
    await this.audit(actor, 'message_core.buttons.upsert', 'message_button_layout', row.id, { communityId, code: input.code, provider: input.provider ?? null });
    return row;
  }

  forms(communityId: string, locale?: string, activeOnly = false) {
    return this.prisma.messageFormDefinition.findMany({
      where: { communityId, ...(locale ? { locale } : {}), ...(activeOnly ? { isActive: true } : {}) },
      orderBy: [{ code: 'asc' }, { locale: 'asc' }],
    });
  }

  async upsertForm(communityId: string, input: { code: string; title: string; locale: string; fields: BotFormField[]; submitTemplateKey?: string | null; successTemplateKey?: string | null; isActive: boolean }, actor: Principal) {
    const row = await this.prisma.messageFormDefinition.upsert({
      where: { communityId_code_locale: { communityId, code: input.code, locale: input.locale } },
      create: {
        communityId,
        code: input.code,
        title: input.title,
        locale: input.locale,
        fields: input.fields as never,
        submitTemplateKey: input.submitTemplateKey ?? null,
        successTemplateKey: input.successTemplateKey ?? null,
        isActive: input.isActive,
      },
      update: {
        title: input.title,
        fields: input.fields as never,
        submitTemplateKey: input.submitTemplateKey ?? null,
        successTemplateKey: input.successTemplateKey ?? null,
        isActive: input.isActive,
      },
    });
    await this.audit(actor, 'message_core.form.upsert', 'message_form_definition', row.id, { communityId, code: input.code });
    return row;
  }

  async submitForm(communityId: string, code: string, locale: string, input: { provider?: MessageProvider | null; userExternalId?: string | null; payload: Record<string, unknown> }) {
    const form = await this.prisma.messageFormDefinition.findUnique({ where: { communityId_code_locale: { communityId, code, locale } } })
      ?? await this.prisma.messageFormDefinition.findFirst({ where: { communityId, code, isActive: true } });
    if (!form || !form.isActive) throw new NotFoundException('Форма не найдена или выключена');
    const validation = validateBotForm(form.fields as unknown as BotFormField[], input.payload);
    const row = await this.prisma.messageFormSubmission.create({
      data: {
        communityId,
        formId: form.id,
        provider: input.provider ?? null,
        userExternalId: input.userExternalId ?? null,
        payload: input.payload as never,
        normalized: validation.normalized as never,
        errors: validation.errors as never,
        status: validation.ok ? 'accepted' : 'rejected',
      },
    });
    return { form, submission: row, validation };
  }

  formSubmissions(communityId: string, formId?: string, take = 50) {
    return this.prisma.messageFormSubmission.findMany({
      where: { communityId, ...(formId ? { formId } : {}) },
      orderBy: { createdAt: 'desc' },
      take: Math.min(take, 500),
    });
  }

  async render(communityId: string, input: RenderInput) {
    const source = await this.resolveSourceText(communityId, input);
    const buttons = input.buttons ?? (input.buttonLayoutCode ? await this.resolveButtons(communityId, input.buttonLayoutCode, input.locale, input.provider) : undefined);
    const rendered = renderStructuredMessage({
      text: source.text,
      locale: input.locale,
      provider: input.provider,
      variables: input.variables,
      buttons,
      parseMode: input.parseMode,
      missingVariables: input.missingVariables,
    });
    const log = await this.prisma.messageRenderLog.create({
      data: {
        communityId,
        templateKey: source.key,
        locale: input.locale,
        provider: input.provider ?? null,
        variables: input.variables as never,
        text: rendered.text,
        buttons: rendered.rawButtons as never,
      },
    });
    return { ...rendered, source, renderLogId: log.id };
  }

  renderLogs(communityId: string, take = 50) {
    return this.prisma.messageRenderLog.findMany({ where: { communityId }, orderBy: { createdAt: 'desc' }, take: Math.min(take, 500) });
  }

  private async resolveSourceText(communityId: string, input: RenderInput): Promise<{ kind: 'text' | 'template' | 'catalog'; key?: string; text: string }> {
    if (input.text) return { kind: 'text', text: input.text };
    if (input.templateKey) {
      const template = await this.prisma.messageTemplate.findUnique({ where: { key_locale: { key: input.templateKey, locale: input.locale } } })
        ?? await this.prisma.messageTemplate.findUnique({ where: { key_locale: { key: input.templateKey, locale: 'ru' } } });
      if (!template) throw new NotFoundException(`Message template не найден: ${input.templateKey}`);
      return { kind: 'template', key: input.templateKey, text: template.body };
    }
    const catalog = await this.prisma.localeCatalog.findUnique({ where: { communityId_namespace_locale: { communityId, namespace: input.namespace, locale: input.locale } } })
      ?? await this.prisma.localeCatalog.findFirst({ where: { communityId, namespace: input.namespace }, orderBy: { locale: 'asc' } });
    const messages = (catalog?.messages ?? {}) as Record<string, unknown>;
    const value = input.catalogKey ? messages[input.catalogKey] : undefined;
    if (typeof value !== 'string') throw new NotFoundException(`Catalog key не найден: ${input.catalogKey}`);
    return { kind: 'catalog', key: input.catalogKey, text: value };
  }

  private async resolveButtons(communityId: string, code: string, locale: string, provider?: MessageProvider) {
    const row = await findMessageButtonLayout(this.prisma, communityId, code, locale, provider);
    return row?.rows;
  }

  private audit(actor: Principal, action: string, resource: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource, resourceId, metadata: metadata as never } });
  }
}
