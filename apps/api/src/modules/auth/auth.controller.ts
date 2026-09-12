import { Body, Controller, Get, Param, Post, Req, UseGuards } from '@nestjs/common';
import { permissions } from '@cajeerbots/security';
import { parseBody } from '../shared/zod.js';
import { CurrentPrincipal } from '../shared/auth-context.js';
import { RequireScopes } from '../shared/scopes.decorator.js';
import { ScopedAuthGuard } from '../shared/scoped-auth.guard.js';
import { AuthService } from './auth.service.js';
import {
  bootstrapAdminSchema,
  changePasswordSchema,
  confirmTwoFactorSchema,
  createApiKeySchema,
  disableTwoFactorSchema,
  issueTokenSchema,
  loginSchema,
  logoutSchema,
  refreshSchema,
  rotateRecoveryCodesSchema,
} from './auth.schemas.js';
import type { Principal } from '@cajeerbots/security';

@Controller('auth')
export class AuthController {
  constructor(private readonly auth: AuthService) {}

  @Post('token')
  issueToken(@Body() body: unknown) {
    return this.auth.issueToken(parseBody(issueTokenSchema, body));
  }

  @Post('admin/bootstrap')
  bootstrapAdmin(@Body() body: unknown, @Req() request: RequestLike) {
    return this.auth.bootstrapAdmin(parseBody(bootstrapAdminSchema, body), requestContext(request));
  }

  @Post('login')
  login(@Body() body: unknown, @Req() request: RequestLike) {
    return this.auth.login(parseBody(loginSchema, body), requestContext(request));
  }

  @Post('refresh')
  refresh(@Body() body: unknown, @Req() request: RequestLike) {
    return this.auth.refresh(parseBody(refreshSchema, body), requestContext(request));
  }

  @Post('logout')
  logout(@Body() body: unknown, @Req() request: RequestLike) {
    return this.auth.logout(parseBody(logoutSchema, body), requestContext(request));
  }

  @Get('me')
  @UseGuards(ScopedAuthGuard)
  me(@CurrentPrincipal() principal: Principal) {
    return this.auth.me(principal);
  }

  @Get('admin-accounts')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authAccountsManage)
  listAdminAccounts() {
    return this.auth.listAdminAccounts();
  }

  @Post('password/change')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authAccountsManage)
  changePassword(@Body() body: unknown, @CurrentPrincipal() principal: Principal, @Req() request: RequestLike) {
    return this.auth.changePassword(principal, parseBody(changePasswordSchema, body), requestContext(request));
  }

  @Get('sessions')
  @UseGuards(ScopedAuthGuard)
  listSessions(@CurrentPrincipal() principal: Principal) {
    return this.auth.listSessions(principal);
  }

  @Post('sessions/:id/revoke')
  @UseGuards(ScopedAuthGuard)
  revokeSession(@Param('id') id: string, @CurrentPrincipal() principal: Principal, @Req() request: RequestLike) {
    return this.auth.revokeSession(id, principal, requestContext(request));
  }

  @Post('2fa/setup')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authTwoFactorManage)
  setupTwoFactor(@CurrentPrincipal() principal: Principal) {
    return this.auth.setupTwoFactor(principal);
  }

  @Post('2fa/confirm')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authTwoFactorManage)
  confirmTwoFactor(@Body() body: unknown, @CurrentPrincipal() principal: Principal, @Req() request: RequestLike) {
    return this.auth.confirmTwoFactor(principal, parseBody(confirmTwoFactorSchema, body), requestContext(request));
  }

  @Post('2fa/disable')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authTwoFactorManage)
  disableTwoFactor(@Body() body: unknown, @CurrentPrincipal() principal: Principal, @Req() request: RequestLike) {
    return this.auth.disableTwoFactor(principal, parseBody(disableTwoFactorSchema, body), requestContext(request));
  }

  @Post('2fa/recovery-codes/rotate')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authTwoFactorManage)
  rotateRecoveryCodes(@Body() body: unknown, @CurrentPrincipal() principal: Principal, @Req() request: RequestLike) {
    return this.auth.rotateRecoveryCodes(principal, parseBody(rotateRecoveryCodesSchema, body), requestContext(request));
  }

  @Get('api-keys')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authTokensManage)
  listApiKeys() {
    return this.auth.listApiKeys();
  }

  @Post('api-keys')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authTokensManage)
  createApiKey(@Body() body: unknown, @CurrentPrincipal() principal: Principal) {
    return this.auth.createApiKey(parseBody(createApiKeySchema, body), principal);
  }

  @Post('api-keys/:id/revoke')
  @UseGuards(ScopedAuthGuard)
  @RequireScopes(permissions.authTokensManage)
  revokeApiKey(@Param('id') id: string, @CurrentPrincipal() principal: Principal) {
    return this.auth.revokeApiKey(id, principal);
  }
}

interface RequestLike {
  ip?: string;
  headers: Record<string, string | string[] | undefined>;
}

function requestContext(request: RequestLike) {
  const userAgent = request.headers['user-agent'];
  return { ip: request.ip, userAgent: Array.isArray(userAgent) ? userAgent[0] : userAgent };
}
