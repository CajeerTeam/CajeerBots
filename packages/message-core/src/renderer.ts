import { renderProviderButtons, type MessageProvider } from './buttons.js';
import { extractVariables, renderVariables } from './variables.js';

export interface StructuredMessageInput {
  text: string;
  locale?: string;
  provider?: MessageProvider;
  variables?: Record<string, unknown>;
  buttons?: unknown;
  parseMode?: 'plain' | 'markdown' | 'html';
  missingVariables?: 'empty' | 'keep' | 'throw';
}

export interface StructuredMessageOutput {
  text: string;
  locale: string;
  provider?: MessageProvider;
  variablesUsed: string[];
  buttons?: unknown;
  rawButtons?: unknown;
}

export function renderStructuredMessage(input: StructuredMessageInput): StructuredMessageOutput {
  const locale = input.locale ?? 'ru';
  const provider = input.provider;
  const rendered = renderVariables(input.text, input.variables ?? {}, { missing: input.missingVariables ?? 'empty' });
  const text = provider ? normalizeProviderText(provider, rendered, input.parseMode ?? 'plain') : rendered;
  return {
    text,
    locale,
    provider,
    variablesUsed: extractVariables(input.text),
    buttons: provider ? renderProviderButtons(provider, input.buttons) : input.buttons,
    rawButtons: input.buttons,
  };
}

export function normalizeProviderText(provider: MessageProvider, text: string, parseMode: 'plain' | 'markdown' | 'html' = 'plain'): string {
  if (parseMode === 'html') return text;
  if (parseMode === 'markdown') return text;
  if (provider === 'discord') return text.slice(0, 2000);
  if (provider === 'telegram') return text.slice(0, 4096);
  return text.slice(0, 4096);
}
