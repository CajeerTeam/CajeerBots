export interface DebugPrismaLike {
  [key: string]: any;
}

export interface RecordDebugEventInput {
  sessionId?: string | null;
  communityId?: string | null;
  level?: 'debug' | 'info' | 'warn' | 'error' | string;
  category: string;
  message: string;
  metadata?: Record<string, unknown>;
}

export async function recordDebugEvent(prisma: DebugPrismaLike, input: RecordDebugEventInput): Promise<any> {
  return prisma.debugEvent.create({
    data: {
      sessionId: input.sessionId ?? null,
      communityId: input.communityId ?? null,
      level: input.level ?? 'info',
      category: input.category,
      message: input.message,
      metadata: (input.metadata ?? {}) as never,
    },
  });
}

export function redactDebugMetadata(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(redactDebugMetadata);
  if (!value || typeof value !== 'object') return value;
  const redacted: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(value as Record<string, unknown>)) {
    if (/token|secret|password|authorization|cookie|signature/i.test(key)) {
      redacted[key] = '[redacted]';
    } else {
      redacted[key] = redactDebugMetadata(inner);
    }
  }
  return redacted;
}
