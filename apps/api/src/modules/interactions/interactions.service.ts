import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import type { Principal } from '@cajeerbots/security';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';
import type { z } from 'zod';
import type {
  createCallbackActionSchema,
  createConversationFlowSchema,
  createInteractionCommandSchema,
  createInteractionMenuSchema,
  recordInteractionEventSchema,
  updateCallbackActionSchema,
  updateConversationFlowSchema,
  updateInteractionCommandSchema,
  updateInteractionMenuSchema,
} from './interactions.schemas.js';

type CreateCommand = z.infer<typeof createInteractionCommandSchema>;
type UpdateCommand = z.infer<typeof updateInteractionCommandSchema>;
type CreateMenu = z.infer<typeof createInteractionMenuSchema>;
type UpdateMenu = z.infer<typeof updateInteractionMenuSchema>;
type CreateCallback = z.infer<typeof createCallbackActionSchema>;
type UpdateCallback = z.infer<typeof updateCallbackActionSchema>;
type CreateFlow = z.infer<typeof createConversationFlowSchema>;
type UpdateFlow = z.infer<typeof updateConversationFlowSchema>;
type RecordInteractionEvent = z.infer<typeof recordInteractionEventSchema>;

@Injectable()
export class InteractionsService {
  private readonly extensionRuntime: DbExtensionRuntime;

  constructor(private readonly prisma: PrismaService) {
    this.extensionRuntime = new DbExtensionRuntime(prisma);
  }

  commands(communityId: string, provider?: 'telegram' | 'vk' | 'discord') {
    return this.prisma.interactionCommand.findMany({
      where: { communityId, ...(provider ? { OR: [{ provider }, { provider: null }] } : {}) },
      orderBy: [{ name: 'asc' }],
    });
  }

  async getCommand(communityId: string, id: string) {
    const command = await this.prisma.interactionCommand.findFirst({ where: { id, communityId } });
    if (!command) throw new NotFoundException('Interaction command не найдена');
    return command;
  }

  async createCommand(communityId: string, input: CreateCommand, actor: Principal) {
    const duplicate = await this.prisma.interactionCommand.findFirst({ where: { communityId, name: input.name, provider: input.provider ?? null } });
    if (duplicate) throw new ConflictException('Команда с таким name/provider уже существует');
    try {
      const command = await this.prisma.interactionCommand.create({ data: { communityId, ...toPrisma(input) } });
      await this.audit(actor, 'interaction.command.create', 'interaction_command', command.id, { communityId, name: command.name });
      return command;
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Команда с таким name/provider уже существует');
      throw error;
    }
  }

  async updateCommand(communityId: string, id: string, input: UpdateCommand, actor: Principal) {
    await this.getCommand(communityId, id);
    const command = await this.prisma.interactionCommand.update({ where: { id }, data: toPrisma(input) });
    await this.audit(actor, 'interaction.command.update', 'interaction_command', id, { communityId, fields: Object.keys(input) });
    return command;
  }

  menus(communityId: string, locale?: string) {
    return this.prisma.interactionMenu.findMany({ where: { communityId, ...(locale ? { locale } : {}) }, orderBy: [{ code: 'asc' }, { locale: 'asc' }] });
  }

  async getMenu(communityId: string, id: string) {
    const menu = await this.prisma.interactionMenu.findFirst({ where: { id, communityId } });
    if (!menu) throw new NotFoundException('Interaction menu не найдено');
    return menu;
  }

  async createMenu(communityId: string, input: CreateMenu, actor: Principal) {
    try {
      const menu = await this.prisma.interactionMenu.create({ data: { communityId, ...toPrisma(input), buttons: input.buttons as never } });
      await this.audit(actor, 'interaction.menu.create', 'interaction_menu', menu.id, { communityId, code: menu.code, locale: menu.locale });
      return menu;
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Меню с таким code/locale уже существует');
      throw error;
    }
  }

  async updateMenu(communityId: string, id: string, input: UpdateMenu, actor: Principal) {
    await this.getMenu(communityId, id);
    const menu = await this.prisma.interactionMenu.update({ where: { id }, data: { ...toPrisma(input), ...(input.buttons ? { buttons: input.buttons as never } : {}) } });
    await this.audit(actor, 'interaction.menu.update', 'interaction_menu', id, { communityId, fields: Object.keys(input) });
    return menu;
  }

  callbacks(communityId: string) {
    return this.prisma.interactionCallbackAction.findMany({ where: { communityId }, orderBy: { code: 'asc' } });
  }

  async getCallback(communityId: string, id: string) {
    const action = await this.prisma.interactionCallbackAction.findFirst({ where: { id, communityId } });
    if (!action) throw new NotFoundException('Callback action не найден');
    return action;
  }

  async createCallback(communityId: string, input: CreateCallback, actor: Principal) {
    try {
      const action = await this.prisma.interactionCallbackAction.create({ data: { communityId, ...toPrisma(input), ...(input.payload ? { payload: input.payload as never } : {}) } });
      await this.audit(actor, 'interaction.callback.create', 'interaction_callback_action', action.id, { communityId, code: action.code });
      return action;
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Callback action с таким code уже существует');
      throw error;
    }
  }

  async updateCallback(communityId: string, id: string, input: UpdateCallback, actor: Principal) {
    await this.getCallback(communityId, id);
    const action = await this.prisma.interactionCallbackAction.update({ where: { id }, data: { ...toPrisma(input), ...(input.payload ? { payload: input.payload as never } : {}) } });
    await this.audit(actor, 'interaction.callback.update', 'interaction_callback_action', id, { communityId, fields: Object.keys(input) });
    return action;
  }

  conversations(communityId: string) {
    return this.prisma.conversationFlow.findMany({ where: { communityId }, orderBy: { code: 'asc' } });
  }

  async getConversation(communityId: string, id: string) {
    const flow = await this.prisma.conversationFlow.findFirst({ where: { id, communityId } });
    if (!flow) throw new NotFoundException('Conversation flow не найден');
    return flow;
  }

  async createConversation(communityId: string, input: CreateFlow, actor: Principal) {
    try {
      const flow = await this.prisma.conversationFlow.create({ data: { communityId, ...toPrisma(input), steps: input.steps as never } });
      await this.audit(actor, 'interaction.conversation.create', 'conversation_flow', flow.id, { communityId, code: flow.code });
      return flow;
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Conversation flow с таким code уже существует');
      throw error;
    }
  }

  async updateConversation(communityId: string, id: string, input: UpdateFlow, actor: Principal) {
    await this.getConversation(communityId, id);
    const flow = await this.prisma.conversationFlow.update({ where: { id }, data: { ...toPrisma(input), ...(input.steps ? { steps: input.steps as never } : {}) } });
    await this.audit(actor, 'interaction.conversation.update', 'conversation_flow', id, { communityId, fields: Object.keys(input) });
    return flow;
  }

  events(communityId: string, take = 50) {
    const limit = Math.min(Math.max(take, 1), 200);
    return this.prisma.interactionEvent.findMany({ where: { communityId }, orderBy: { createdAt: 'desc' }, take: limit });
  }

  async recordEvent(communityId: string, input: RecordInteractionEvent) {
    const event = await this.prisma.interactionEvent.create({
      data: {
        communityId,
        accountId: input.accountId ?? null,
        provider: input.provider,
        type: input.type,
        name: input.name ?? null,
        userExternalId: input.userExternalId,
        status: input.status,
        payload: input.payload as never,
        response: input.response as never,
        error: input.error ?? null,
      },
    });
    const extensionEvents = await this.extensionRuntime.enqueueHook({ hook: 'interaction.event', payload: { communityId, event } });
    return { ...event, extensionEvents };
  }

  private audit(actor: Principal, action: string, resource: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource, resourceId, metadata: metadata as never } });
  }
}

function toPrisma<T extends Record<string, unknown>>(input: T): T {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined)) as T;
}

function isUniqueError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === 'P2002';
}
