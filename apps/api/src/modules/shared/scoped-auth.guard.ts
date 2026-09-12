import { CanActivate, ExecutionContext, Injectable, UnauthorizedException, ForbiddenException } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { PrismaService } from '@cajeerbots/db';
import { hashApiToken, hasScope, verifyJwt, type Principal } from '@cajeerbots/security';
import { getHeader, type AuthenticatedRequest } from './auth-context.js';
import { SCOPES_KEY } from './scopes.decorator.js';

@Injectable()
export class ScopedAuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly prisma: PrismaService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const required = this.reflector.getAllAndOverride<string[]>(SCOPES_KEY, [context.getHandler(), context.getClass()]) ?? [];
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const principal = await this.authenticate(request);
    request.principal = principal;

    if (required.length === 0) return true;
    const denied = required.filter((scope) => !hasScope(principal, scope));
    if (denied.length > 0) throw new ForbiddenException({ code: 'SCOPE_DENIED', required: denied });
    return true;
  }

  private async authenticate(request: AuthenticatedRequest): Promise<Principal> {
    const authorization = getHeader(request.headers, 'authorization');
    const bearer = authorization?.startsWith('Bearer ') ? authorization.slice('Bearer '.length) : undefined;
    const xApiKey = getHeader(request.headers, 'x-api-key');
    const adminToken = process.env.CAJEER_ADMIN_TOKEN;

    if (adminToken && (xApiKey === adminToken || bearer === adminToken)) {
      return { id: 'bootstrap-admin', type: 'system', scopes: ['*'] };
    }

    if (bearer?.includes('.')) {
      const payload = verifyJwt(bearer, process.env.JWT_SECRET ?? '');
      if (payload) return { id: payload.sub, type: payload.typ, scopes: payload.scopes };
    }

    const rawApiToken = xApiKey ?? (bearer && !bearer.includes('.') ? bearer : undefined);
    if (rawApiToken) {
      const pepper = process.env.API_TOKEN_PEPPER;
      if (!pepper) throw new UnauthorizedException('API_TOKEN_PEPPER не задан');
      const keyHash = hashApiToken(rawApiToken, pepper);
      const apiKey = await this.prisma.apiKey.findFirst({
        where: {
          keyHash,
          revokedAt: null,
          OR: [{ expiresAt: null }, { expiresAt: { gt: new Date() } }],
        },
      });
      if (apiKey) {
        await this.prisma.apiKey.update({ where: { id: apiKey.id }, data: { lastUsedAt: new Date() } });
        return { id: apiKey.id, type: 'api_key', scopes: apiKey.scopes };
      }
    }

    throw new UnauthorizedException('Нужен JWT Bearer token или x-api-key с подходящими scopes');
  }
}
