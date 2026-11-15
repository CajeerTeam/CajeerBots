export interface BotAdapter {
  isConfigured(): boolean;
  /** Local connection state; webhook accounts are monitored separately. */
  isHealthy?(): boolean;
  start(): Promise<void>;
  stop(): Promise<void>;
}
