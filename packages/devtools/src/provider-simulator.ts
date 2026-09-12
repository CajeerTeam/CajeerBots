import { randomUUID } from 'node:crypto';

export type SimulatedProvider = 'telegram' | 'vk' | 'discord';

export interface SimulateProviderUpdateInput {
  provider: SimulatedProvider;
  accountId: string;
  communityId: string;
  type?: 'message' | 'command' | 'callback' | 'form' | string;
  text?: string;
  command?: string;
  callbackCode?: string;
  userExternalId?: string;
  chatExternalId?: string;
  payload?: Record<string, unknown>;
  providerUpdateId?: string;
}

export interface ProviderUpdateEnvelope {
  provider: SimulatedProvider;
  providerUpdateId: string;
  updateType: string;
  userExternalId: string;
  chatExternalId: string;
  payload: Record<string, unknown>;
}

export function buildProviderUpdate(input: SimulateProviderUpdateInput): ProviderUpdateEnvelope {
  const type = input.type ?? (input.command ? 'command' : input.callbackCode ? 'callback' : 'message');
  const userExternalId = input.userExternalId ?? 'dev-user';
  const chatExternalId = input.chatExternalId ?? userExternalId;
  const providerUpdateId = input.providerUpdateId ?? `dev-${Date.now()}-${randomUUID()}`;
  const basePayload = input.payload ?? {};
  return {
    provider: input.provider,
    providerUpdateId,
    updateType: type,
    userExternalId,
    chatExternalId,
    payload: {
      ...basePayload,
      devtools: true,
      text: input.text ?? (input.command ? `/${input.command}` : ''),
      command: input.command,
      callbackCode: input.callbackCode,
      user: { id: userExternalId },
      chat: { id: chatExternalId },
    },
  };
}

export async function persistSimulatedProviderUpdate(prisma: any, input: SimulateProviderUpdateInput): Promise<any> {
  const envelope = buildProviderUpdate(input);
  const inbound = await prisma.botInboundUpdate.create({
    data: {
      accountId: input.accountId,
      communityId: input.communityId,
      provider: input.provider,
      providerUpdateId: envelope.providerUpdateId,
      type: envelope.updateType,
      userExternalId: envelope.userExternalId,
      chatExternalId: envelope.chatExternalId,
      payload: envelope.payload as never,
      status: 'received',
    },
  });
  await prisma.interactionEvent.create({
    data: {
      communityId: input.communityId,
      accountId: input.accountId,
      provider: input.provider,
      type: envelope.updateType,
      name: input.command ?? input.callbackCode ?? null,
      userExternalId: envelope.userExternalId,
      status: 'received',
      payload: envelope.payload as never,
    },
  });
  return inbound;
}
