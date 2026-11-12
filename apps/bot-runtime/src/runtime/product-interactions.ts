import { createHash, randomBytes } from 'node:crypto';
import type { Prisma, PrismaService } from '@cajeerbots/db';
import { validateBotForm, type BotFormField } from '@cajeerbots/message-core';
import { type ConversationStep, type IncomingInteraction, type InteractionResponse, type PersistedInteractionCommand } from '@cajeerbots/interaction-core';
import type { RuntimeBotAccount } from './load-bot-accounts.js';

type Tx = Prisma.TransactionClient;
type Session = { kind: 'flow' | 'form'; definitionId: string; revision: string; originCommand?: string; originCallback?: string; values: Record<string, unknown>; nonce: string };
const ACTIVE = 'rc4:active';
const ttl = 30 * 60_000;
const reserved = 'rc4:';
function obj(value: unknown): Record<string, unknown> { return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
function str(value: unknown): string | undefined { return typeof value === 'string' && value ? value : undefined; }
function reply(text: string): InteractionResponse { return { text, ephemeral: true }; }
export async function userHasScopes(tx: Tx, account: RuntimeBotAccount, userId: string, required: string[]): Promise<boolean> {
  if (!required.length) return true;
  const user = await tx.user.findUnique({ where: { communityId_provider_externalId: { communityId: account.communityId, provider: account.provider, externalId: userId } }, include: { roles: { include: { role: { include: { permissions: { include: { permission: true } } } } } } } });
  const granted = new Set(user?.roles.filter(({ role }) => role.communityId === account.communityId).flatMap(({ role }) => role.permissions.map(({ permission }) => permission.scope)) ?? []);
  return required.every((scope) => granted.has(scope) || granted.has('*'));
}
export function parseAnswer(interaction: IncomingInteraction): string | undefined {
  if (interaction.type === 'message') return str(interaction.payload.text);
  if (interaction.name !== 'answer') return undefined;
  if (Array.isArray(interaction.payload.args)) return interaction.payload.args.map(String).join(' ');
  if (Array.isArray(interaction.payload.options)) {
    const option = interaction.payload.options.find((value) => obj(value).name === 'value');
    return str(obj(option).value);
  }
  return undefined;
}
function choices(step: ConversationStep | BotFormField) {
  return 'id' in step ? step.choices ?? [] : step.options ?? [];
}
function question(step: ConversationStep | BotFormField, nonce: string): InteractionResponse {
  const isFlow = 'id' in step;
  const options = choices(step);
  const text = isFlow ? step.text : `${step.label}${step.required ? '' : ' (необязательно: /answer /skip)'}${step.type === 'multiselect' ? ' — несколько значений через запятую' : ''}`;
  const templateKey = isFlow ? step.messageKey : undefined;
  const buttons = (isFlow || step.type !== 'multiselect' ? options : []).slice(0, 20).map((option, index) => [{ id: `${index}`, label: option.label.slice(0, 80), action: `${reserved}${nonce}:${index}` }]);
  if (isFlow && (!step.input || step.input === 'none')) buttons.push([{ id: 'next', label: 'Далее', action: `${reserved}${nonce}:next` }]);
  // Discord supports at most five rows. Keep other choices available by exact text via /answer.
  return { ...(text ? { text: options.length ? `${text}\n${options.map((o) => o.value).join(', ')}` : text } : { templateKey: templateKey ?? 'system.help' }), buttons: buttons.slice(0, 5), ephemeral: true };
}
export function validateConversationAnswer(step: ConversationStep, value: string | undefined): { ok: boolean; value?: unknown } {
  if (!step.input || step.input === 'none') return { ok: true };
  if (!value || value.length > 4000) return { ok: false };
  if (step.input === 'number') return Number.isFinite(Number(value)) && value.trim() !== '' ? { ok: true, value: Number(value) } : { ok: false };
  if (step.input === 'boolean') {
    if (['true', '1', 'да', 'yes'].includes(value.toLowerCase())) return { ok: true, value: true };
    if (['false', '0', 'нет', 'no'].includes(value.toLowerCase())) return { ok: true, value: false };
    return { ok: false };
  }
  if (step.input === 'choice' && !step.choices?.some((o) => o.value === value)) return { ok: false };
  return { ok: true, value };
}
export async function dispatchProductInteraction(prisma: PrismaService, account: RuntimeBotAccount, interaction: IncomingInteraction, command: PersistedInteractionCommand | null, targetId: string | undefined, inboundId?: string): Promise<InteractionResponse | null | undefined> {
  if (!targetId || interaction.userId === 'unknown') return undefined;
  const communityId = account.communityId;
  const userId = createHash('sha256').update(JSON.stringify([account.id, targetId, interaction.userId])).digest('hex');
  const sessionKey = { communityId, provider: account.provider, userId, flow: ACTIVE };
  const where = { communityId_provider_userId_flow: sessionKey };
  const replayKey = inboundId ? `rc4:interaction:${inboundId}` : undefined;
  return prisma.$transaction(async (tx) => {
    await tx.$queryRaw`SELECT 1 FROM pg_advisory_xact_lock(hashtextextended(${communityId + ':' + userId}, 0))`;
    if (replayKey) {
      const replay = await tx.idempotencyKey.findUnique({ where: { key: replayKey } });
      if (replay) return (obj(replay.responseBody).response ?? null) as InteractionResponse | null;
    }
    const result = await run();
    if (result !== undefined && replayKey) await tx.idempotencyKey.create({ data: { key: replayKey, scope: 'bot-interaction', requestHash: userId, responseBody: { response: result } as never, expiresAt: new Date(Date.now() + 7 * 86400_000) } });
    return result;

    async function save(step: string, session: Session) {
      await tx.conversationState.upsert({ where, create: { ...sessionKey, step, data: JSON.parse(JSON.stringify(session)) as never, expiresAt: new Date(Date.now() + ttl) }, update: { step, data: JSON.parse(JSON.stringify(session)) as never, expiresAt: new Date(Date.now() + ttl) } });
    }
    async function clear() { await tx.conversationState.deleteMany({ where: sessionKey }); }
    async function run(): Promise<InteractionResponse | null | undefined> {
      const name = interaction.name ?? '';
      const isCallback = interaction.type === 'callback';
      const callback = isCallback && !name.startsWith(reserved) ? await tx.interactionCallbackAction.findUnique({ where: { communityId_code: { communityId, code: name } } }) : null;
      if (isCallback && !name.startsWith(reserved) && (!callback?.isActive || !await userHasScopes(tx, account, interaction.userId, callback.scopes))) return null;
      const row = await tx.conversationState.findUnique({ where });
      const state = row && row.expiresAt && row.expiresAt > new Date() ? row : null;
      if (row && !state) await clear();
      if (!isCallback && name === 'cancel' && interaction.type !== 'message') { await clear(); return reply('Диалог отменён.'); }
      const answer = parseAnswer(interaction);
      if (state && (interaction.type === 'message' || (!isCallback && name === 'answer') || (isCallback && name.startsWith(reserved)))) {
        const session = state.data as unknown as Session;
        const origin = session.originCommand ? await tx.interactionCommand.findFirst({ where: { id: session.originCommand, communityId, isEnabled: true, OR: [{ provider: account.provider }, { provider: null }] } }) : null;
        const originCallback = session.originCallback ? await tx.interactionCallbackAction.findFirst({ where: { id: session.originCallback, communityId, isActive: true } }) : null;
        if ((session.originCommand && !origin) || (session.originCallback && !originCallback) || !await userHasScopes(tx, account, interaction.userId, [...(origin?.scopes ?? []), ...(originCallback?.scopes ?? [])])) { await clear(); return reply('Доступ к диалогу отозван.'); }
        let value = answer;
        const button = isCallback ? name.split(':') : undefined;
        if (button && (button.length !== 3 || button[1] !== session.nonce)) return reply('Кнопка устарела.');
        if (session.kind === 'flow') {
          const flow = await tx.conversationFlow.findFirst({ where: { id: session.definitionId, communityId, isActive: true } });
          if (!flow || flow.updatedAt.toISOString() !== session.revision) { await clear(); return reply('Диалог изменён. Начните заново.'); }
          const steps = flow.steps as unknown as ConversationStep[];
          const step = steps.find((s) => s.id === state.step);
          if (!step) { await clear(); return reply('Диалог изменён.'); }
          if (button) { if (button[2] !== 'next' && !/^\d+$/.test(button[2] ?? '')) return reply('Кнопка недоступна.'); value = choices(step)[Number(button[2])]?.value; }
          const validated = validateConversationAnswer(step, value);
          if (!validated.ok) return { ...question(step, session.nonce), text: `Некорректный ответ. ${step.text ?? 'Повторите ввод.'}` };
          if (step.variable && !['__proto__', 'constructor', 'prototype'].includes(step.variable) && validated.value !== undefined) session.values[step.variable] = validated.value;
          const next = !step.complete && step.next ? steps.find((s) => s.id === step.next) : undefined;
          if (!next && !step.complete && step.next) { await clear(); return reply('Диалог настроен неверно.'); }
          if (!next) { await clear(); await tx.outboxMessage.create({ data: { communityId, topic: 'conversation.completed', payload: { communityId, flow: flow.code, provider: account.provider, userId: interaction.userId, values: session.values } as never } }); return reply('Диалог завершён.'); }
          session.nonce = randomBytes(8).toString('hex'); await save(next.id, session); return question(next, session.nonce);
        }
        const form = await tx.messageFormDefinition.findFirst({ where: { id: session.definitionId, communityId, isActive: true } });
        if (!form || form.updatedAt.toISOString() !== session.revision) { await clear(); return reply('Форма изменена. Начните заново.'); }
        const fields = form.fields as unknown as BotFormField[];
        const field = fields[Number(state.step)];
        if (!field) { await clear(); return reply('Форма изменена.'); }
        if (button) { if (!/^\d+$/.test(button[2] ?? '')) return reply('Кнопка недоступна.'); value = choices(field)[Number(button[2])]?.value; }
        if (value === undefined || value.length > 4000) return question(field, session.nonce);
        const validation = validateBotForm([field], { [field.name]: value === '/skip' && !field.required ? '' : value });
        if (!validation.ok) return { ...question(field, session.nonce), text: validation.errors.map((e) => e.message).join('\n') + '\n' + field.label };
        Object.assign(session.values, validation.normalized);
        const next = fields[Number(state.step) + 1];
        if (next) { session.nonce = randomBytes(8).toString('hex'); await save(String(Number(state.step) + 1), session); return question(next, session.nonce); }
        const all = validateBotForm(fields, session.values);
        if (!all.ok) { await clear(); return reply('Форма не прошла проверку. Начните заново.'); }
        await tx.messageFormSubmission.create({ data: { communityId, formId: form.id, provider: account.provider, userExternalId: interaction.userId, payload: session.values as never, normalized: all.normalized as never, errors: [], status: 'accepted' } });
        await clear(); return form.successTemplateKey ? { templateKey: form.successTemplateKey, ephemeral: true } : reply('Форма принята.');
      }
      if (isCallback && name.startsWith(reserved)) return reply('Диалог истёк. Начните заново.');
      if (name === 'answer' && interaction.type !== 'message') return reply('Нет активного диалога.');
      const handler = callback?.handler ?? command?.handler;
      const metadata = obj(callback?.payload ?? command?.metadata);
      const code = str(metadata.code) ?? str(metadata.menuCode) ?? str(metadata.formCode) ?? str(metadata.conversationCode) ?? name;
      if (command?.cooldownSeconds) {
        const key = { ...sessionKey, flow: `rc4:cooldown:${command.id}` };
        const unique = { communityId_provider_userId_flow: key };
        const existing = await tx.conversationState.findUnique({ where: unique });
        if (existing?.expiresAt && existing.expiresAt > new Date()) return reply('Команда временно недоступна. Повторите позже.');
        const expiresAt = new Date(Date.now() + command.cooldownSeconds * 1000);
        await tx.conversationState.upsert({ where: unique, create: { ...key, step: 'cooldown', data: {}, expiresAt }, update: { expiresAt } });
      }
      const language = (await tx.community.findUnique({ where: { id: communityId }, select: { language: true } }))?.language ?? 'ru';
      if (handler === 'menu.open') {
        const menu = await tx.interactionMenu.findFirst({ where: { communityId, code, locale: language, isActive: true } }) ?? await tx.interactionMenu.findFirst({ where: { communityId, code, locale: 'ru', isActive: true } });
        return menu ? { ...(menu.templateKey ? { templateKey: menu.templateKey } : { text: menu.title }), buttons: menu.buttons as unknown as InteractionResponse['buttons'], ephemeral: true } : reply('Меню недоступно.');
      }
      if (handler === 'form.start') {
        const form = await tx.messageFormDefinition.findFirst({ where: { communityId, code, locale: language, isActive: true } }) ?? await tx.messageFormDefinition.findFirst({ where: { communityId, code, locale: 'ru', isActive: true } });
        const first = (form?.fields as unknown as BotFormField[] | undefined)?.[0];
        if (!form || !first) return reply('Форма недоступна.');
        const nonce = randomBytes(8).toString('hex'); await save('0', { kind: 'form', definitionId: form.id, revision: form.updatedAt.toISOString(), originCommand: command?.id, originCallback: callback?.id, values: {}, nonce }); return question(first, nonce);
      }
      const flowCode = command?.conversationCode ?? (handler === 'conversation.start' ? code : undefined);
      const flow = flowCode ? await tx.conversationFlow.findFirst({ where: { communityId, code: flowCode, isActive: true } }) : !isCallback && interaction.type !== 'message' && !command ? await tx.conversationFlow.findFirst({ where: { communityId, entryCommand: name, isActive: true } }) : null;
      if (flow) {
        const first = (flow.steps as unknown as ConversationStep[])[0];
        if (!first) return reply('Диалог недоступен.');
        const nonce = randomBytes(8).toString('hex'); await save(first.id, { kind: 'flow', definitionId: flow.id, revision: flow.updatedAt.toISOString(), originCommand: command?.id, originCallback: callback?.id, values: {}, nonce }); return question(first, nonce);
      }
      if (flowCode) return reply('Диалог недоступен.');
      if (callback) {
        if (handler === 'template' && str(metadata.templateKey)) return { templateKey: String(metadata.templateKey), ephemeral: true };
        return null; // Never interpret arbitrary callback data as a command.
      }
      return undefined;
    }
  });
}
