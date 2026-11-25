/** Explicit per-key channel allowlist, shared by API and worker. No token values here. */
export const workspacePrefix = 'workspace-';
export const workspaceScope = 'workspace:messages';
export type WorkspaceTarget = { communityId: string; accountId: string; provider: 'telegram' | 'vk'; channelId: string };
export const workspaceIdentifier = (s: unknown): s is string => typeof s === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(s);
export function workspaceTarget(key: string, community: string): WorkspaceTarget | null {
  try {
    const raw = process.env.WORKSPACE_BOT_TARGETS ?? '{}';
    if (raw.length > 262144 || !workspaceIdentifier(key) || !workspaceIdentifier(community)) return null;
    const map = JSON.parse(raw);
    if (!Object.hasOwn(map, key)) return null;
    const t = map[key];
    if (!t || t.communityId !== community || !workspaceIdentifier(t.accountId) || typeof t.channelId !== 'string') return null;
    if (!(t.provider === 'telegram' && /^-100[0-9]{5,16}$/.test(t.channelId) || t.provider === 'vk' && /^[0-9]{1,16}$/.test(t.channelId) && !/^0/.test(t.channelId))) return null;
    return { communityId: community, accountId: t.accountId, provider: t.provider, channelId: t.channelId };
  } catch { return null; }
}
export function workspaceMarker(key: string, revision: string): string { return `workspace:${key}:${revision}`; }
export function workspaceText(text: unknown): text is string {
  return typeof text === 'string' && text.trim().length > 0 && Buffer.byteLength(text, 'utf8') <= 8000 && [...text].length <= 2000 && [...text].every(ch => { const n = ch.codePointAt(0)!; return (n === 9 || n === 10 || n >= 32) && n !== 127 && !(n >= 0xd800 && n <= 0xdfff); });
}
export function workspaceKeyAllowed(scopes: string[], community: string): boolean {
  return scopes.length === 2 && scopes.includes(workspaceScope) && scopes.includes(`community:${community}`);
}
