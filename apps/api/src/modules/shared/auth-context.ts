import { createParamDecorator, type ExecutionContext } from '@nestjs/common';
import type { Principal } from '@cajeerbots/security';

export interface AuthenticatedRequest {
  headers: Record<string, string | string[] | undefined>;
  ip?: string;
  principal?: Principal;
}

export const CurrentPrincipal = createParamDecorator((_data: unknown, context: ExecutionContext): Principal => {
  const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
  return request.principal ?? { id: 'anonymous', type: 'system', scopes: [] };
});

export function getHeader(headers: Record<string, string | string[] | undefined>, name: string): string | undefined {
  const value = headers[name] ?? headers[name.toLowerCase()];
  return Array.isArray(value) ? value[0] : value;
}
