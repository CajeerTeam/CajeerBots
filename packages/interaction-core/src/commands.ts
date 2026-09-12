import type { BotProvider } from '@cajeerbots/core';
import type { IncomingInteraction, InteractionHandler, PersistedInteractionCommand } from './types.js';

export interface CommandDefinition {
  name: string;
  description: string;
  scopes?: string[];
  handler: InteractionHandler;
}

export function normalizeCommandName(value: string): string {
  return value.trim().replace(/^\/+/, '').replaceAll('-', '_').toLowerCase();
}

export function parseCommandFromText(text: string): { name: string; args: string[] } | null {
  const normalized = text.trim();
  if (!normalized.startsWith('/')) return null;
  const [rawName = '', ...args] = normalized.split(/\s+/);
  const name = normalizeCommandName(rawName.split('@')[0] ?? rawName);
  return name ? { name, args } : null;
}

export function commandMatches(command: PersistedInteractionCommand, provider: BotProvider, name: string): boolean {
  if (!command.isEnabled) return false;
  if (command.provider && command.provider !== provider) return false;
  const normalized = normalizeCommandName(name);
  return normalizeCommandName(command.name) === normalized || command.aliases.map(normalizeCommandName).includes(normalized);
}

export function sortCommands(commands: PersistedInteractionCommand[]): PersistedInteractionCommand[] {
  return [...commands].sort((left, right) => left.name.localeCompare(right.name, 'ru'));
}
