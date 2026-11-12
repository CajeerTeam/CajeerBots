import type { Principal } from './rbac.js';

/** Community grants are independent of action scopes; wildcard never escapes a tenant. */
export function canAccessCommunity(principal: Principal, communityId?: string): boolean {
  const grants = principal.scopes.filter((scope) => scope.startsWith('community:'));
  if (grants.length) return communityId !== undefined && grants.includes(`community:${communityId}`);
  return principal.scopes.includes('*') || principal.scopes.includes('platform:global');
}
