export interface BotAdapter {
  isConfigured(): boolean;
  start(): Promise<void>;
  stop(): Promise<void>;
}
