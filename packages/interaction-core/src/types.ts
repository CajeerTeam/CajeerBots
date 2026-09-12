import type { BotProvider } from '@cajeerbots/core';

export type IncomingInteractionType = 'command' | 'message' | 'callback' | 'slash_command' | 'conversation';

export interface IncomingInteraction {
  provider: BotProvider;
  type: IncomingInteractionType;
  userId: string;
  name?: string;
  payload: Record<string, unknown>;
}

export interface InteractionResponse {
  text?: string;
  templateKey?: string;
  variables?: Record<string, string | number | boolean | null>;
  buttons?: Array<Array<{ id: string; label: string; action: string; payload?: Record<string, unknown> }>>;
  ephemeral?: boolean;
  metadata?: Record<string, unknown>;
}

export type InteractionHandler = (interaction: IncomingInteraction) => Promise<InteractionResponse | void> | InteractionResponse | void;

export interface PersistedInteractionCommand {
  id: string;
  communityId: string;
  name: string;
  description: string;
  provider?: BotProvider | null;
  type: string;
  aliases: string[];
  scopes: string[];
  handler: string;
  templateKey?: string | null;
  conversationCode?: string | null;
  cooldownSeconds: number;
  isEnabled: boolean;
  metadata?: unknown;
}

export interface PersistedInteractionMenu {
  id: string;
  communityId: string;
  code: string;
  title: string;
  locale: string;
  templateKey?: string | null;
  buttons: unknown;
  isActive: boolean;
}

export interface PersistedCallbackAction {
  id: string;
  communityId: string;
  code: string;
  title: string;
  handler: string;
  scopes: string[];
  payload?: unknown;
  isActive: boolean;
}
