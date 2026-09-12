import { SetMetadata } from '@nestjs/common';

export const SCOPES_KEY = 'cajeerbots:scopes';

export function RequireScopes(...scopes: string[]) {
  return SetMetadata(SCOPES_KEY, scopes);
}
