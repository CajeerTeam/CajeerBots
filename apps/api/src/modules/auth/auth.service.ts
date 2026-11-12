import { randomBytes } from 'node:crypto';
import { BadRequestException, ConflictException, ForbiddenException, Injectable, NotFoundException, UnauthorizedException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import {
  createApiToken,
  createOtpAuthUri,
  createRecoveryCodes,
  createTotpSecret,
  hashApiToken,
  hashPassword,
  hashRecoveryCode,
  signJwt,
  verifyPassword,
  verifyTotp,
  type Principal,
} from '@cajeerbots/security';

const DEFAULT_ADMIN_SCOPES = ['*'];
const DEFAULT_REFRESH_TTL_SECONDS = 60 * 60 * 24 * 30;

@Injectable()
export class AuthService {
  constructor(private readonly prisma: PrismaService) {}

  async issueToken(input: { adminToken?: string; scopes: string[]; ttlSeconds?: number }) {
    const expected = process.env.CAJEER_ADMIN_TOKEN;
    if (!expected || input.adminToken !== expected) throw new UnauthorizedException('Некорректный bootstrap admin token');

    const ttlSeconds = input.ttlSeconds ?? Number(process.env.JWT_ACCESS_TTL_SECONDS ?? 3600);
    const issued = signJwt({ sub: 'bootstrap-admin', typ: 'system', scopes: normalizeScopes(input.scopes) }, requireJwtSecret(), ttlSeconds);
    await this.audit({ actorType: 'system', actorId: 'bootstrap-admin', action: 'auth.jwt.issue', resource: 'auth_token', metadata: { scopes: input.scopes, expiresAt: issued.expiresAt } });
    return issued;
  }

  async bootstrapAdmin(input: { adminToken?: string; email: string; password: string; displayName?: string; scopes: string[] }, context: RequestContext = {}) {
    const expected = process.env.CAJEER_ADMIN_TOKEN;
    if (!expected || input.adminToken !== expected) throw new UnauthorizedException('Некорректный bootstrap admin token');

    const accounts = await this.prisma.adminAccount.count();
    if (accounts > 0 && input.adminToken !== expected) throw new ForbiddenException('Bootstrap admin доступен только с CAJEER_ADMIN_TOKEN');

    const exists = await this.prisma.adminAccount.findUnique({ where: { email: input.email } });
    if (exists) throw new ConflictException('Admin account с таким email уже существует');

    const account = await this.prisma.adminAccount.create({
      data: {
        email: input.email,
        displayName: input.displayName,
        passwordHash: hashPassword(input.password, { pepper: passwordPepper() }),
        scopes: normalizeScopes(input.scopes.length ? input.scopes : DEFAULT_ADMIN_SCOPES),
      },
      select: adminAccountSelect(),
    });
    await this.audit({ actorType: 'system', actorId: 'bootstrap-admin', action: 'auth.admin.bootstrap', resource: 'admin_account', resourceId: account.id, ip: context.ip, userAgent: context.userAgent, metadata: { email: account.email, scopes: account.scopes } });
    return account;
  }

  async login(input: { email: string; password: string; code?: string; recoveryCode?: string }, context: RequestContext = {}) {
    const account = await this.prisma.adminAccount.findUnique({ where: { email: input.email } });
    if (!account || !account.isActive || !verifyPassword(input.password, account.passwordHash, { pepper: passwordPepper() })) {
      await this.audit({ actorType: 'anonymous', action: 'auth.login.failed', resource: 'admin_account', ip: context.ip, userAgent: context.userAgent, metadata: { email: input.email, reason: 'invalid_credentials' } });
      throw new UnauthorizedException('Некорректный email или пароль');
    }

    if (account.twoFactorEnabled) await this.verifySecondFactor(account.id, input.code, input.recoveryCode, context);

    await this.prisma.adminAccount.update({ where: { id: account.id }, data: { lastLoginAt: new Date() } });
    const session = await this.issueAdminSession(account.id, account.scopes, context);
    await this.audit({ actorType: 'user', actorId: account.id, action: 'auth.login.success', resource: 'admin_session', resourceId: session.session.id, ip: context.ip, userAgent: context.userAgent });
    return { account: sanitizeAdminAccount(account), ...session };
  }

  async refresh(input: { refreshToken: string }, context: RequestContext = {}) {
    const refreshPepper = refreshTokenPepper();
    const refreshHash = hashApiToken(input.refreshToken, refreshPepper);
    const now = new Date();
    const session = await this.prisma.adminSession.findFirst({
      where: { refreshTokenHash: refreshHash, revokedAt: null, expiresAt: { gt: now } },
      include: { account: true },
    });
    if (!session || !session.account.isActive) throw new UnauthorizedException('Refresh session недействительна');

    const rotated = createRefreshToken();
    const access = signJwt({ sub: session.accountId, typ: 'user', sid: session.id, scopes: normalizeScopes(session.account.scopes) }, requireJwtSecret(), accessTtlSeconds());
    const changed = await this.prisma.adminSession.updateMany({ where: { id: session.id, refreshTokenHash: refreshHash, revokedAt: null, expiresAt: { gt: now } }, data: { refreshTokenHash: hashApiToken(rotated, refreshPepper), lastUsedAt: now, userAgent: context.userAgent ?? session.userAgent, ip: context.ip ?? session.ip } });
    if (!changed.count) throw new UnauthorizedException('Refresh session недействительна');
    const updated = await this.prisma.adminSession.findUniqueOrThrow({ where: { id: session.id }, select: adminSessionSelect() });
    await this.audit({ actorType: 'user', actorId: session.accountId, action: 'auth.session.refresh', resource: 'admin_session', resourceId: session.id, ip: context.ip, userAgent: context.userAgent });
    return { accessToken: access.token, accessTokenExpiresAt: access.expiresAt, refreshToken: rotated, refreshTokenExpiresAt: updated.expiresAt, session: updated };
  }

  async logout(input: { refreshToken: string }, context: RequestContext = {}) {
    const refreshHash = hashApiToken(input.refreshToken, refreshTokenPepper());
    const session = await this.prisma.adminSession.findUnique({ where: { refreshTokenHash: refreshHash } });
    if (!session) return { revoked: false };
    await this.prisma.adminSession.update({ where: { id: session.id }, data: { revokedAt: new Date() } });
    await this.audit({ actorType: 'user', actorId: session.accountId, action: 'auth.logout', resource: 'admin_session', resourceId: session.id, ip: context.ip, userAgent: context.userAgent });
    return { revoked: true };
  }

  listApiKeys() {
    return this.prisma.apiKey.findMany({
      orderBy: { createdAt: 'desc' },
      select: { id: true, title: true, scopes: true, expiresAt: true, lastUsedAt: true, createdAt: true, revokedAt: true },
    });
  }

  async createApiKey(input: { title: string; scopes: string[]; expiresAt?: string }, actor: Principal) {
    const pepper = process.env.API_TOKEN_PEPPER;
    if (!pepper) throw new BadRequestException('API_TOKEN_PEPPER не задан');
    if (!actor.scopes.includes('*') && input.scopes.some((scope) => !actor.scopes.includes(scope))) throw new ForbiddenException('Нельзя делегировать отсутствующие полномочия');
    const material = createApiToken(pepper);
    const created = await this.prisma.apiKey.create({
      data: { title: input.title, keyHash: material.hash, scopes: normalizeScopes(input.scopes), ...(input.expiresAt ? { expiresAt: new Date(input.expiresAt) } : {}) },
      select: { id: true, title: true, scopes: true, expiresAt: true, createdAt: true },
    });
    await this.audit({ actorType: actor.type, actorId: actor.id, action: 'auth.api_key.create', resource: 'api_key', resourceId: created.id, metadata: { title: created.title, scopes: created.scopes, expiresAt: created.expiresAt } });
    return { ...created, token: material.token };
  }

  async revokeApiKey(id: string, actor: Principal) {
    const existing = await this.prisma.apiKey.findUnique({ where: { id } });
    if (!existing) throw new NotFoundException('API key не найден');
    const updated = await this.prisma.apiKey.update({ where: { id }, data: { revokedAt: new Date() } });
    await this.audit({ actorType: actor.type, actorId: actor.id, action: 'auth.api_key.revoke', resource: 'api_key', resourceId: id });
    return updated;
  }

  async me(principal: Principal) {
    if (principal.type !== 'user') return principal;
    const account = await this.prisma.adminAccount.findUnique({ where: { id: principal.id }, select: adminAccountSelect() });
    return { principal, account };
  }

  async listAdminAccounts() {
    return this.prisma.adminAccount.findMany({ orderBy: { createdAt: 'desc' }, select: adminAccountSelect() });
  }

  async listSessions(principal: Principal) {
    const canManageAll = principal.scopes.includes('*') || principal.scopes.includes('auth:sessions:manage');
    return this.prisma.adminSession.findMany({
      where: canManageAll ? {} : { accountId: principal.id },
      orderBy: { createdAt: 'desc' },
      select: adminSessionSelect(),
    });
  }

  async revokeSession(id: string, principal: Principal, context: RequestContext = {}) {
    const session = await this.prisma.adminSession.findUnique({ where: { id } });
    if (!session) throw new NotFoundException('Session не найдена');
    const canManageAll = principal.scopes.includes('*') || principal.scopes.includes('auth:sessions:manage');
    if (!canManageAll && session.accountId !== principal.id) throw new ForbiddenException('Нельзя отозвать чужую session');
    const updated = await this.prisma.adminSession.update({ where: { id }, data: { revokedAt: new Date() }, select: adminSessionSelect() });
    await this.audit({ actorType: principal.type, actorId: principal.id, action: 'auth.session.revoke', resource: 'admin_session', resourceId: id, ip: context.ip, userAgent: context.userAgent });
    return updated;
  }

  async changePassword(principal: Principal, input: { currentPassword: string; newPassword: string; revokeOtherSessions: boolean }, context: RequestContext = {}) {
    if (principal.type !== 'user') throw new ForbiddenException('Смена пароля доступна только admin account');
    const account = await this.prisma.adminAccount.findUnique({ where: { id: principal.id } });
    if (!account || !verifyPassword(input.currentPassword, account.passwordHash, { pepper: passwordPepper() })) throw new UnauthorizedException('Некорректный текущий пароль');
    await this.prisma.adminAccount.update({ where: { id: account.id }, data: { passwordHash: hashPassword(input.newPassword, { pepper: passwordPepper() }) } });
    if (input.revokeOtherSessions) await this.prisma.adminSession.updateMany({ where: { accountId: account.id, revokedAt: null }, data: { revokedAt: new Date() } });
    await this.audit({ actorType: principal.type, actorId: principal.id, action: 'auth.password.change', resource: 'admin_account', resourceId: account.id, ip: context.ip, userAgent: context.userAgent, metadata: { revokeOtherSessions: input.revokeOtherSessions } });
    return { changed: true, revokedSessions: input.revokeOtherSessions };
  }

  async setupTwoFactor(principal: Principal) {
    if (principal.type !== 'user') throw new ForbiddenException('2FA доступна только admin account');
    const account = await this.prisma.adminAccount.findUnique({ where: { id: principal.id } });
    if (!account) throw new NotFoundException('Admin account не найден');
    const secret = createTotpSecret();
    await this.prisma.twoFactorSecret.updateMany({ where: { accountId: account.id, isActive: false, confirmedAt: null }, data: { disabledAt: new Date() } });
    const created = await this.prisma.twoFactorSecret.create({ data: { accountId: account.id, secret } });
    return { id: created.id, secret, otpauthUri: createOtpAuthUri({ issuer: process.env.TOTP_ISSUER ?? 'CajeerBots', accountName: account.email, secret }) };
  }

  async confirmTwoFactor(principal: Principal, input: { code: string }, context: RequestContext = {}) {
    if (principal.type !== 'user') throw new ForbiddenException('2FA доступна только admin account');
    const pending = await this.prisma.twoFactorSecret.findFirst({ where: { accountId: principal.id, confirmedAt: null, disabledAt: null }, orderBy: { createdAt: 'desc' } });
    if (!pending || !verifyTotp(input.code, pending.secret)) throw new UnauthorizedException('Некорректный TOTP code');
    const codes = createRecoveryCodes(10);
    await this.prisma.$transaction([
      this.prisma.twoFactorSecret.updateMany({ where: { accountId: principal.id, isActive: true }, data: { isActive: false, disabledAt: new Date() } }),
      this.prisma.twoFactorSecret.update({ where: { id: pending.id }, data: { isActive: true, confirmedAt: new Date() } }),
      this.prisma.recoveryCode.deleteMany({ where: { accountId: principal.id, usedAt: null } }),
      ...codes.map((code) => this.prisma.recoveryCode.create({ data: { accountId: principal.id, codeHash: hashRecoveryCode(code, recoveryCodePepper()) } })),
      this.prisma.adminAccount.update({ where: { id: principal.id }, data: { twoFactorEnabled: true, twoFactorConfirmedAt: new Date() } }),
    ]);
    await this.audit({ actorType: principal.type, actorId: principal.id, action: 'auth.2fa.enable', resource: 'admin_account', resourceId: principal.id, ip: context.ip, userAgent: context.userAgent });
    return { enabled: true, recoveryCodes: codes };
  }

  async disableTwoFactor(principal: Principal, input: { password: string; code?: string; recoveryCode?: string }, context: RequestContext = {}) {
    if (principal.type !== 'user') throw new ForbiddenException('2FA доступна только admin account');
    const account = await this.prisma.adminAccount.findUnique({ where: { id: principal.id } });
    if (!account || !verifyPassword(input.password, account.passwordHash, { pepper: passwordPepper() })) throw new UnauthorizedException('Некорректный пароль');
    if (account.twoFactorEnabled) await this.verifySecondFactor(account.id, input.code, input.recoveryCode, context);
    await this.prisma.$transaction([
      this.prisma.twoFactorSecret.updateMany({ where: { accountId: principal.id, isActive: true }, data: { isActive: false, disabledAt: new Date() } }),
      this.prisma.recoveryCode.deleteMany({ where: { accountId: principal.id } }),
      this.prisma.adminAccount.update({ where: { id: principal.id }, data: { twoFactorEnabled: false, twoFactorConfirmedAt: null } }),
    ]);
    await this.audit({ actorType: principal.type, actorId: principal.id, action: 'auth.2fa.disable', resource: 'admin_account', resourceId: principal.id, ip: context.ip, userAgent: context.userAgent });
    return { enabled: false };
  }

  async rotateRecoveryCodes(principal: Principal, input: { password: string; code?: string; recoveryCode?: string }, context: RequestContext = {}) {
    if (principal.type !== 'user') throw new ForbiddenException('2FA доступна только admin account');
    const account = await this.prisma.adminAccount.findUnique({ where: { id: principal.id } });
    if (!account || !verifyPassword(input.password, account.passwordHash, { pepper: passwordPepper() })) throw new UnauthorizedException('Некорректный пароль');
    if (account.twoFactorEnabled) await this.verifySecondFactor(account.id, input.code, input.recoveryCode, context);
    const codes = createRecoveryCodes(10);
    await this.prisma.$transaction([
      this.prisma.recoveryCode.deleteMany({ where: { accountId: principal.id, usedAt: null } }),
      ...codes.map((code) => this.prisma.recoveryCode.create({ data: { accountId: principal.id, codeHash: hashRecoveryCode(code, recoveryCodePepper()) } })),
    ]);
    await this.audit({ actorType: principal.type, actorId: principal.id, action: 'auth.2fa.recovery_codes.rotate', resource: 'admin_account', resourceId: principal.id, ip: context.ip, userAgent: context.userAgent });
    return { recoveryCodes: codes };
  }

  private async verifySecondFactor(accountId: string, code?: string, recoveryCode?: string, context: RequestContext = {}) {
    if (code) {
      const active = await this.prisma.twoFactorSecret.findFirst({ where: { accountId, isActive: true, disabledAt: null }, orderBy: { confirmedAt: 'desc' } });
      if (active && verifyTotp(code, active.secret)) return;
    }
    if (recoveryCode) {
      const codeHash = hashRecoveryCode(recoveryCode, recoveryCodePepper());
      const record = await this.prisma.recoveryCode.findFirst({ where: { accountId, codeHash, usedAt: null } });
      if (record) {
        const claimed = await this.prisma.recoveryCode.updateMany({ where: { id: record.id, usedAt: null }, data: { usedAt: new Date() } });
        if (!claimed.count) throw new UnauthorizedException('Recovery code already used');
        await this.audit({ actorType: 'user', actorId: accountId, action: 'auth.2fa.recovery_code.used', resource: 'admin_account', resourceId: accountId, ip: context.ip, userAgent: context.userAgent });
        return;
      }
    }
    await this.audit({ actorType: 'user', actorId: accountId, action: 'auth.2fa.failed', resource: 'admin_account', resourceId: accountId, ip: context.ip, userAgent: context.userAgent });
    throw new UnauthorizedException({ code: 'TWO_FACTOR_REQUIRED', message: 'Нужен корректный TOTP code или recovery code' });
  }

  private async issueAdminSession(accountId: string, scopes: string[], context: RequestContext) {
    const refreshToken = createRefreshToken();
    const refreshTokenExpiresAt = new Date(Date.now() + refreshTtlSeconds() * 1000);
    const session = await this.prisma.adminSession.create({
      data: {
        accountId,
        refreshTokenHash: hashApiToken(refreshToken, refreshTokenPepper()),
        expiresAt: refreshTokenExpiresAt,
        ip: context.ip,
        userAgent: context.userAgent,
      },
      select: adminSessionSelect(),
    });
    const access = signJwt({ sub: accountId, typ: 'user', sid: session.id, scopes: normalizeScopes(scopes) }, requireJwtSecret(), accessTtlSeconds());
    return { accessToken: access.token, accessTokenExpiresAt: access.expiresAt, refreshToken, refreshTokenExpiresAt, session };
  }

  private audit(input: { actorType: string; actorId?: string; action: string; resource: string; resourceId?: string; ip?: string; userAgent?: string; metadata?: unknown }) {
    return this.prisma.auditLog.create({ data: { ...input, metadata: input.metadata as never } });
  }
}

export interface RequestContext {
  ip?: string;
  userAgent?: string;
}

function normalizeScopes(scopes: string[]): string[] {
  return Array.from(new Set(scopes.map((scope) => scope.trim()).filter(Boolean)));
}

function sanitizeAdminAccount(account: { id: string; email: string; displayName: string | null; scopes: string[]; isActive: boolean; twoFactorEnabled: boolean; createdAt: Date; updatedAt: Date; lastLoginAt: Date | null }) {
  return {
    id: account.id,
    email: account.email,
    displayName: account.displayName,
    scopes: account.scopes,
    isActive: account.isActive,
    twoFactorEnabled: account.twoFactorEnabled,
    createdAt: account.createdAt,
    updatedAt: account.updatedAt,
    lastLoginAt: account.lastLoginAt,
  };
}

function adminAccountSelect() {
  return { id: true, email: true, displayName: true, scopes: true, isActive: true, twoFactorEnabled: true, twoFactorConfirmedAt: true, lastLoginAt: true, createdAt: true, updatedAt: true } as const;
}

function adminSessionSelect() {
  return { id: true, accountId: true, ip: true, userAgent: true, expiresAt: true, lastUsedAt: true, revokedAt: true, createdAt: true } as const;
}

function createRefreshToken(): string {
  return `cjr_refresh_${randomBytes(32).toString('base64url')}`;
}

function requireJwtSecret(): string {
  const value = process.env.JWT_SECRET;
  if (!value) throw new BadRequestException('JWT_SECRET не задан');
  return value;
}

function passwordPepper(): string {
  return process.env.PASSWORD_PEPPER ?? process.env.API_TOKEN_PEPPER ?? '';
}

function refreshTokenPepper(): string {
  const value = process.env.REFRESH_TOKEN_PEPPER ?? process.env.API_TOKEN_PEPPER;
  if (!value) throw new BadRequestException('REFRESH_TOKEN_PEPPER или API_TOKEN_PEPPER не задан');
  return value;
}

function recoveryCodePepper(): string {
  return process.env.RECOVERY_CODE_PEPPER ?? refreshTokenPepper();
}

function accessTtlSeconds(): number {
  return Number(process.env.JWT_ACCESS_TTL_SECONDS ?? 3600);
}

function refreshTtlSeconds(): number {
  return Number(process.env.JWT_REFRESH_TTL_SECONDS ?? DEFAULT_REFRESH_TTL_SECONDS);
}
