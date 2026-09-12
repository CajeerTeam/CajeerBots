export type BotProvider = 'telegram' | 'vk' | 'discord';
export type BotMode = 'polling' | 'webhook';
export type LocaleCode = 'ru' | string;

export interface ActorRef {
  id: string;
  type: 'user' | 'api_key' | 'system' | 'extension';
}
