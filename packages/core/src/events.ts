export interface EventEnvelope<TPayload = unknown> {
  id: string;
  type: string;
  occurredAt: string;
  payload: TPayload;
  metadata?: Record<string, unknown>;
}

export interface DomainEvent<TPayload = unknown> extends EventEnvelope<TPayload> {
  aggregateId?: string;
  aggregateType?: string;
}
