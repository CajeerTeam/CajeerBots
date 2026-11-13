import { renderStructuredMessage } from '@cajeerbots/message-core';
import { PrismaService } from '@cajeerbots/db';
import { InteractionRouter, commandMatches, type IncomingInteraction, type InteractionResponse } from '@cajeerbots/interaction-core';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';
import type { RuntimeBotAccount } from './load-bot-accounts.js';
import { dispatchProductInteraction } from './product-interactions.js';
import { createOutboundMessage, markInboundHandled } from './runtime-records.js';

export interface RenderedBotResponse {
  text: string;
  buttons?: InteractionResponse['buttons'];
  ephemeral?: boolean;
  outboundMessageId?: string;
  targetId?: string;
}

export async function configureRouterFromDatabase(prisma: PrismaService, router: InteractionRouter): Promise<void> {
  const commands = await prisma.interactionCommand.findMany({ where: { isEnabled: true }, orderBy: [{ name: 'asc' }] });
  router.loadCommands(commands);

  router.register('status', async (interaction) => ({
    templateKey: 'system.status',
    variables: { status: 'ok', version: process.env.npm_package_version ?? '1.0.1', communityId: String(interaction.payload.communityId ?? '') },
  }));

  router.register('help', async (interaction) => ({
    templateKey: 'system.help',
    variables: { communityId: String(interaction.payload.communityId ?? ''), provider: interaction.provider },
  }));

  router.register('settings', async (interaction) => ({
    templateKey: 'system.settings',
    variables: { communityId: String(interaction.payload.communityId ?? ''), locale: 'ru' },
  }));

  router.register('debug', async (interaction) => {
    const communityId = String(interaction.payload.communityId ?? '');
    const [inbound, outbound, failedInteractions, activeSessions] = await Promise.all([
      prisma.botInboundUpdate.count({ where: { communityId } }),
      prisma.botOutboundMessage.count({ where: { communityId } }),
      prisma.interactionEvent.count({ where: { communityId, status: 'failed' } }),
      prisma.debugSession.count({ where: { communityId, status: 'active' } }),
    ]);
    return {
      text: `Debug CajeerBots\ncommunity=${communityId}\ninbound=${inbound}\noutbound=${outbound}\nfailed_interactions=${failedInteractions}\nactive_debug_sessions=${activeSessions}`,
      variables: { communityId, inbound, outbound, failedInteractions, activeSessions },
    };
  });
}

export async function dispatchRuntimeInteraction(
  prisma: PrismaService,
  router: InteractionRouter,
  account: RuntimeBotAccount,
  interaction: Omit<IncomingInteraction, 'provider'> & { provider?: RuntimeBotAccount['provider'] },
  options: { inboundUpdateId?: string; targetId?: string } = {},
): Promise<RenderedBotResponse | undefined> {
  const normalized: IncomingInteraction = {
    provider: account.provider,
    type: interaction.type,
    name: interaction.name,
    userId: interaction.userId,
    payload: { ...interaction.payload, communityId: account.communityId, accountId: account.id },
  };

  const event = await prisma.interactionEvent.create({
    data: {
      communityId: account.communityId,
      accountId: account.id.startsWith('env:') ? null : account.id,
      provider: normalized.provider,
      type: normalized.type,
      name: normalized.name ?? null,
      userExternalId: normalized.userId,
      status: 'received',
      payload: normalized.payload as never,
    },
  });

  try {
    if (!account.id.startsWith('env:')) {
      const active = await prisma.botAccount.findFirst({ where: { id: account.id, communityId: account.communityId, provider: account.provider, mode: account.mode, isActive: true, tokenRef: account.tokenRef } });
      if (!active) throw new Error('Bot account disabled');
    }
    const commands = await prisma.interactionCommand.findMany({ where: { communityId: account.communityId, OR: [{ provider: account.provider }, { provider: null }] } });
    const command = normalized.type === 'callback' ? undefined : commands.sort((a, b) => Number(b.provider === account.provider) - Number(a.provider === account.provider)).find((entry) => commandMatches({ ...entry, isEnabled: true }, normalized.provider, normalized.name ?? ''));
    const callback = normalized.type === 'callback' && normalized.name ? await prisma.interactionCallbackAction.findUnique({ where: { communityId_code: { communityId: account.communityId, code: normalized.name } } }) : null;
    if (command?.isEnabled === false || callback?.isActive === false) {
      await prisma.interactionEvent.update({ where: { id: event.id }, data: { status: 'ignored' } });
      if (options.inboundUpdateId) await markInboundHandled(prisma, options.inboundUpdateId, 'ignored');
      return undefined;
    }
    const required = [...(command?.scopes ?? []), ...(callback?.scopes ?? [])];
    if (normalized.name === 'debug' || command?.handler === 'debug') required.push('bot_runtime:read');
    if (required.length) {
      const user = await prisma.user.findUnique({ where: { communityId_provider_externalId: { communityId: account.communityId, provider: account.provider, externalId: normalized.userId } }, include: { roles: { include: { role: { include: { permissions: { include: { permission: true } } } } } } } });
      const granted = new Set(user?.roles.filter(({ role }) => role.communityId === account.communityId).flatMap(({ role }) => role.permissions.map(({ permission }) => permission.scope)) ?? []);
      if (!required.every((scope) => granted.has(scope) || granted.has('*'))) {
        await prisma.interactionEvent.update({ where: { id: event.id }, data: { status: 'ignored' } });
        if (options.inboundUpdateId) await markInboundHandled(prisma, options.inboundUpdateId, 'ignored');
        return undefined;
      }
    }
    const product = await dispatchProductInteraction(prisma, account, normalized, command ?? null, options.targetId ?? resolveTargetId(normalized.payload), options.inboundUpdateId);
    const response = product === undefined ? await router.dispatch(normalized, command ?? null) : product ?? undefined;
    const rendered = response ? await renderResponse(prisma, account.communityId, response, account.provider) : undefined;
    if (rendered?.text && account.provider !== 'discord') {
      const targetId = options.targetId ?? resolveTargetId(normalized.payload);
      if (targetId) {
        const outbound = await createOutboundMessage(prisma, account, { inboundUpdateId: options.inboundUpdateId, targetId, text: rendered.text, buttons: rendered.buttons });
        rendered.outboundMessageId = outbound.id;
        rendered.targetId = targetId;
      }
    }
    const updated = await prisma.interactionEvent.update({ where: { id: event.id }, data: { status: rendered ? 'handled' : 'ignored', response: rendered as never } });
    await new DbExtensionRuntime(prisma).enqueueHook({ hook: 'interaction.handled', idempotencyKey: options.inboundUpdateId || event.id, payload: { communityId: account.communityId, event: updated, interaction: normalized, response: rendered } });
    if (options.inboundUpdateId) await markInboundHandled(prisma, options.inboundUpdateId, rendered ? 'handled' : 'ignored');
    return rendered;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const failed = await prisma.interactionEvent.update({ where: { id: event.id }, data: { status: 'failed', error: message } });
    if (options.inboundUpdateId) await markInboundHandled(prisma, options.inboundUpdateId, 'failed', message);
    await new DbExtensionRuntime(prisma).enqueueHook({ hook: 'interaction.failed', payload: { communityId: account.communityId, event: failed, interaction: normalized, error: message } });
    return undefined;
  }
}

export async function renderResponse(prisma: PrismaService, communityId: string, response: InteractionResponse, provider?: RuntimeBotAccount['provider']): Promise<RenderedBotResponse> {
  const community = await prisma.community.findUnique({ where: { id: communityId } });
  const locale = String(response.variables?.locale ?? community?.language ?? 'ru');
  let body = response.text;
  let templateKey: string | undefined;
  let buttons = response.buttons;

  if (!body) {
    templateKey = response.templateKey ?? 'system.help';
    const template = await prisma.messageTemplate.findUnique({ where: { key_locale: { key: templateKey, locale } } })
      ?? await prisma.messageTemplate.findUnique({ where: { key_locale: { key: templateKey, locale: 'ru' } } });
    body = template?.body ?? templateKey;
    buttons = buttons ?? (template?.buttons as InteractionResponse['buttons'] | undefined);
  }

  const rendered = renderStructuredMessage({ text: body ?? '', locale, provider, variables: response.variables ?? {}, buttons });
  await prisma.messageRenderLog.create({
    data: {
      communityId,
      templateKey: templateKey ?? response.templateKey ?? null,
      locale,
      provider: provider ?? null,
      variables: (response.variables ?? {}) as never,
      text: rendered.text,
      buttons: rendered.rawButtons as never,
    },
  }).catch(() => undefined);

  return { text: rendered.text, buttons: rendered.rawButtons as InteractionResponse['buttons'] | undefined, ephemeral: response.ephemeral };
}

function resolveTargetId(payload: Record<string, unknown>): string | undefined {
  const raw = payload.chatId ?? payload.peerId ?? payload.channelId ?? payload.guildId;
  return raw === undefined || raw === null ? undefined : String(raw);
}
