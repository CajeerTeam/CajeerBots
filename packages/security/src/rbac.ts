import { PermissionDeniedError } from '@cajeerbots/core';
import type { PermissionScope } from './permissions.js';

export interface Principal {
  id: string;
  type: 'user' | 'api_key' | 'system' | 'extension';
  scopes: string[];
}

export function hasScope(principal: Principal, scope: PermissionScope | string): boolean {
  return principal.scopes.includes('*') || principal.scopes.includes(scope);
}

export function assertScope(principal: Principal, scope: PermissionScope | string): void {
  if (!hasScope(principal, scope)) {
    throw new PermissionDeniedError(scope);
  }
}

export function parseScopes(value: string | string[] | undefined): string[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.flatMap((item) => parseScopes(item));
  return value.split(/[\s,]+/).map((item) => item.trim()).filter(Boolean);
}
