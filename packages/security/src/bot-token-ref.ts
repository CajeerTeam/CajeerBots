/** Environment credentials are restricted to provider-specific operator-managed names. */
export function validBotTokenRef(provider: string, ref: string, communityId?: string): boolean {
  if (ref.length > 400 || !['telegram', 'vk', 'discord'].includes(provider)) return false;
  if (/^secret:bot\.[A-Za-z0-9_.:-]+$/.test(ref)) {
    if (communityId === undefined) return true; // schema-only validation; runtime always binds the community
    const prefix = `secret:bot.${communityId}.${provider}.`;
    return ref.startsWith(prefix) && ref.length > prefix.length;
  }
  const prefix = `${provider.toUpperCase()}_BOT_TOKEN`;
  return ref === `env:${prefix}` || new RegExp(`^env:${prefix}_[A-Z0-9_]+$`).test(ref);
}
export function assertBotTokenRef(provider: string, ref: string, communityId: string): void {
  if (!validBotTokenRef(provider, ref, communityId)) throw new Error('Invalid provider token reference');
}
