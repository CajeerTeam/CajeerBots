import { renderVariables } from './variables.js';

export interface MessageCatalog {
  locale: string;
  messages: Record<string, string>;
  fallbackLocale?: string;
}

export class Localizer {
  constructor(private readonly catalogs: MessageCatalog[]) {}

  t(locale: string, key: string, variables: Record<string, unknown> = {}): string {
    const template = this.resolve(locale, key) ?? key;
    return renderVariables(template, variables);
  }

  resolve(locale: string, key: string): string | undefined {
    const exact = this.catalogs.find((item) => item.locale === locale);
    if (exact?.messages[key] !== undefined) return exact.messages[key];
    if (exact?.fallbackLocale) {
      const fallback = this.catalogs.find((item) => item.locale === exact.fallbackLocale);
      if (fallback?.messages[key] !== undefined) return fallback.messages[key];
    }
    return this.catalogs[0]?.messages[key];
  }
}
