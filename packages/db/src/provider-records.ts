import type { Prisma } from '../../../prisma/generated/client/index.js';

// PostgreSQL treats NULLs as distinct in ordinary unique indexes. Prisma's
// compound unique selectors therefore require a non-null provider. Serialize
// the nullable case within the caller's transaction and update by primary key.
async function lockProviderRecord(tx: Prisma.TransactionClient, key: unknown[]) {
  const lockKey = JSON.stringify(key);
  await tx.$queryRaw`SELECT 1 FROM pg_advisory_xact_lock(hashtextextended(${lockKey}, 0))`;
}

export async function upsertInteractionCommand(
  tx: Prisma.TransactionClient,
  create: Prisma.InteractionCommandUncheckedCreateInput,
  update: Prisma.InteractionCommandUncheckedUpdateInput,
) {
  const { communityId, name, provider } = create;
  if (provider != null) {
    return tx.interactionCommand.upsert({ where: { communityId_provider_name: { communityId, name, provider } }, create, update });
  }
  await lockProviderRecord(tx, ['interaction-command', communityId, name]);
  const existing = await tx.interactionCommand.findFirst({ where: { communityId, name, provider: null }, orderBy: { id: 'asc' } });
  return existing
    ? tx.interactionCommand.update({ where: { id: existing.id }, data: update })
    : tx.interactionCommand.create({ data: { ...create, provider: null } });
}

export async function upsertMessageButtonLayout(
  tx: Prisma.TransactionClient,
  create: Prisma.MessageButtonLayoutUncheckedCreateInput,
  update: Prisma.MessageButtonLayoutUncheckedUpdateInput,
) {
  const { communityId, code, provider, locale = 'ru' } = create;
  if (provider != null) {
    return tx.messageButtonLayout.upsert({ where: { communityId_code_provider_locale: { communityId, code, provider, locale } }, create, update });
  }
  await lockProviderRecord(tx, ['message-button-layout', communityId, code, locale]);
  const existing = await tx.messageButtonLayout.findFirst({ where: { communityId, code, provider: null, locale }, orderBy: { id: 'asc' } });
  return existing
    ? tx.messageButtonLayout.update({ where: { id: existing.id }, data: update })
    : tx.messageButtonLayout.create({ data: { ...create, provider: null, locale } });
}

export async function findMessageButtonLayout(
  tx: Pick<Prisma.TransactionClient, 'messageButtonLayout'>,
  communityId: string,
  code: string,
  locale: string,
  provider?: NonNullable<Prisma.MessageButtonLayoutUncheckedCreateInput['provider']>,
) {
  // A generic layout is a fallback, not a wildcard for another provider.
  for (const candidateLocale of new Set([locale, 'ru'])) {
    for (const candidateProvider of provider == null ? [null] : [provider, null]) {
      const row = await tx.messageButtonLayout.findFirst({
        where: { communityId, code, provider: candidateProvider, locale: candidateLocale },
        orderBy: { id: 'asc' },
      });
      if (row) return row;
    }
  }
  return null;
}
