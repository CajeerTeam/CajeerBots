import type { ExtensionManifest } from './manifest.schema.js';

export interface ExtensionEventEnvelope<TPayload = unknown> {
  id: string;
  hook: string;
  payload: TPayload;
  extension: {
    id: string;
    name: string;
    version: string;
  };
  createdAt: string;
}

export interface ExtensionLogger {
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}

export interface ExtensionContext {
  manifest: ExtensionManifest;
  logger: ExtensionLogger;
  events: {
    publish(type: string, payload: unknown): Promise<void>;
    enqueueHook(hook: string, payload: unknown): Promise<void>;
  };
  storage: {
    get<T = unknown>(namespace: string, key: string): Promise<T | undefined>;
    set(namespace: string, key: string, value: unknown): Promise<void>;
    delete(namespace: string, key: string): Promise<void>;
  };
}

export interface CajeerExtension {
  activate?(context: ExtensionContext): Promise<void> | void;
  deactivate?(context: ExtensionContext): Promise<void> | void;
  handleEvent?(context: ExtensionContext, event: ExtensionEventEnvelope): Promise<unknown> | unknown;
}
