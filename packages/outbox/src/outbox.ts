import type { EventEnvelope } from '@cajeerbots/core';

export interface OutboxPublisher {
  enqueue(event: EventEnvelope, options?: { topic?: string; headers?: Record<string, unknown> }): Promise<string>;
}

export interface OutboxMessageRecord {
  id: string;
  topic: string;
  payload: unknown;
  headers: unknown;
  attempts: number;
}

interface PrismaLikeOutboxClient {
  outboxMessage: {
    create(args: { data: { topic: string; communityId?: string; payload: unknown; headers?: unknown } }): Promise<{ id: string }>;
  };
}

export class PrismaOutboxPublisher implements OutboxPublisher {
  constructor(private readonly prisma: PrismaLikeOutboxClient) {}

  async enqueue(event: EventEnvelope, options: { topic?: string; headers?: Record<string, unknown> } = {}): Promise<string> {
    const created = await this.prisma.outboxMessage.create({
      data: {
        topic: options.topic ?? event.type,
        communityId: typeof event.metadata?.communityId === 'string' ? event.metadata.communityId : undefined,
        payload: event,
        headers: options.headers ?? {},
      },
    });
    return created.id;
  }
}

export class InMemoryOutboxPublisher implements OutboxPublisher {
  readonly events: EventEnvelope[] = [];

  async enqueue(event: EventEnvelope): Promise<string> {
    this.events.push(event);
    return event.id;
  }
}
