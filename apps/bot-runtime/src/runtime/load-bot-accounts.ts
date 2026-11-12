import { PrismaService } from '@cajeerbots/db';
import { resolveTokenRef } from '../secrets/token-resolver.js';

export interface RuntimeBotAccount {
  id: string;
  communityId: string;
  provider: 'telegram' | 'vk' | 'discord';
  mode: 'polling' | 'webhook';
  username?: string | null;
  token: string;
  tokenRef?: string | null;
}

export async function loadRuntimeBotAccounts(prisma: PrismaService, fallbackMode: 'polling' | 'webhook'): Promise<RuntimeBotAccount[]> {
  const accounts = await prisma.botAccount.findMany({ where: { isActive: true }, orderBy: { createdAt: 'asc' } });
  const loaded: RuntimeBotAccount[] = [];
  const identities = new Set<string>();
  for (const account of accounts) {
    const token = await resolveTokenRef(prisma, account.tokenRef);
    if (!token) {
      console.warn(`[bot-runtime] account=${account.id} provider=${account.provider} пропущен: tokenRef пустой или не резолвится`);
      continue;
    }
    const identity = `${account.provider}:${token}`;
    if (identities.has(identity)) throw new Error('Duplicate bot credentials across accounts');
    identities.add(identity);
    loaded.push({ tokenRef: account.tokenRef, id: account.id, communityId: account.communityId, provider: account.provider, mode: account.mode, username: account.username, token });
  }

  if (process.env.NODE_ENV === 'production') return loaded;
  const fallbackCommunityId = await resolveFallbackCommunityId(prisma);
  if (!fallbackCommunityId) return loaded;

  const envFallbacks: Array<{ provider: RuntimeBotAccount['provider']; token?: string }> = [
    { provider: 'telegram', token: process.env.TELEGRAM_BOT_TOKEN },
    { provider: 'vk', token: process.env.VK_BOT_TOKEN },
    { provider: 'discord', token: process.env.DISCORD_BOT_TOKEN },
  ];
  for (const fallback of envFallbacks) {
    if (!fallback.token) continue;
    loaded.push({ id: `env:${fallback.provider}`, communityId: fallbackCommunityId, provider: fallback.provider, mode: fallbackMode, token: fallback.token });
  }
  return loaded;
}

async function resolveFallbackCommunityId(prisma: PrismaService): Promise<string | undefined> {
  if (!process.env.CAJEER_DEFAULT_COMMUNITY_ID) return undefined;
  const community = await prisma.community.findUnique({ where: { id: process.env.CAJEER_DEFAULT_COMMUNITY_ID }, select: { id: true } });
  return community?.id;
}
