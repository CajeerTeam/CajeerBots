import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';
import { getHeader, type AuthenticatedRequest } from './auth-context.js';

@Injectable()
export class AdminTokenGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const expected = process.env.CAJEER_ADMIN_TOKEN;
    if (!expected && process.env.NODE_ENV !== 'production') return true;
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const apiKey = getHeader(request.headers, 'x-api-key');
    const authorization = getHeader(request.headers, 'authorization');
    const bearer = authorization?.startsWith('Bearer ') ? authorization.slice('Bearer '.length) : undefined;
    if (expected && (apiKey === expected || bearer === expected)) return true;
    throw new UnauthorizedException('Нужен bootstrap x-api-key или Bearer token администратора CajeerBots');
  }
}
