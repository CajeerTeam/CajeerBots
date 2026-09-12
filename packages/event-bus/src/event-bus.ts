import type { DomainEvent } from '@cajeerbots/core';

export type EventHandler<T = unknown> = (event: DomainEvent<T>) => Promise<void> | void;

export class EventBus {
  private readonly handlers = new Map<string, EventHandler[]>();

  subscribe(type: string, handler: EventHandler): void {
    const list = this.handlers.get(type) ?? [];
    list.push(handler);
    this.handlers.set(type, list);
  }

  async publish(event: DomainEvent): Promise<void> {
    const list = this.handlers.get(event.type) ?? [];
    await Promise.all(list.map((handler) => handler(event)));
  }
}
