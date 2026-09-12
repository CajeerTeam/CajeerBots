import { createHash, randomUUID } from 'node:crypto';
import type { EventEnvelope } from '@cajeerbots/core';

export interface DurableEventInput<TPayload = unknown> {
  id?: string;
  type: string;
  topic?: string;
  occurredAt?: string;
  source?: string;
  communityId?: string | null;
  aggregateType?: string | null;
  aggregateId?: string | null;
  payload: TPayload;
  metadata?: Record<string, unknown>;
  headers?: Record<string, unknown>;
}

export interface EventSubscriptionFilter {
  communityId?: string;
  source?: string;
  aggregateType?: string;
  aggregateId?: string;
  metadata?: Record<string, unknown>;
}

export function createDurableEventEnvelope<TPayload = unknown>(input: DurableEventInput<TPayload>): EventEnvelope<TPayload> {
  return {
    id: input.id ?? randomUUID(),
    type: input.type,
    occurredAt: input.occurredAt ?? new Date().toISOString(),
    payload: input.payload,
    metadata: {
      ...(input.metadata ?? {}),
      topic: input.topic ?? input.type,
      source: input.source ?? 'api',
      communityId: input.communityId ?? undefined,
      aggregateType: input.aggregateType ?? undefined,
      aggregateId: input.aggregateId ?? undefined,
    },
  };
}

export function eventFingerprint(input: Pick<DurableEventInput, 'type' | 'topic' | 'source' | 'communityId' | 'aggregateType' | 'aggregateId' | 'payload'>): string {
  return createHash('sha256')
    .update(JSON.stringify({
      type: input.type,
      topic: input.topic ?? input.type,
      source: input.source ?? 'api',
      communityId: input.communityId ?? null,
      aggregateType: input.aggregateType ?? null,
      aggregateId: input.aggregateId ?? null,
      payload: input.payload,
    }))
    .digest('hex');
}

export function matchesTopicPattern(pattern: string, topic: string): boolean {
  if (pattern === '*' || pattern === topic) return true;
  if (pattern.endsWith('.*')) return topic.startsWith(pattern.slice(0, -1));
  if (pattern.includes('*')) {
    const escaped = pattern.split('*').map(escapeRegex).join('.*');
    return new RegExp(`^${escaped}$`).test(topic);
  }
  return false;
}

export function subscriptionAllowsEvent(filter: unknown, event: {
  communityId?: string | null;
  source?: string | null;
  aggregateType?: string | null;
  aggregateId?: string | null;
  metadata?: unknown;
}): boolean {
  const parsed = parseFilter(filter);
  if (!parsed) return true;
  if (parsed.communityId && parsed.communityId !== event.communityId) return false;
  if (parsed.source && parsed.source !== event.source) return false;
  if (parsed.aggregateType && parsed.aggregateType !== event.aggregateType) return false;
  if (parsed.aggregateId && parsed.aggregateId !== event.aggregateId) return false;
  if (parsed.metadata) {
    const metadata = typeof event.metadata === 'object' && event.metadata !== null ? event.metadata as Record<string, unknown> : {};
    for (const [key, expected] of Object.entries(parsed.metadata)) {
      if (metadata[key] !== expected) return false;
    }
  }
  return true;
}

export function nextBackoffDate(attempts: number, baseMs = 1000, maxMs = 300_000): Date {
  return new Date(Date.now() + Math.min(maxMs, Math.max(baseMs, 2 ** Math.max(0, attempts) * baseMs)));
}

function parseFilter(value: unknown): EventSubscriptionFilter | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  return value as EventSubscriptionFilter;
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
