import { z } from 'zod';

const scope = z.string().min(1).max(120);
const password = z.string().min(10).max(512);
const email = z.string().email().max(320).transform((value) => value.toLowerCase());

export const issueTokenSchema = z.object({
  adminToken: z.string().optional(),
  scopes: z.array(scope).default(['*']),
  ttlSeconds: z.number().int().positive().max(60 * 60 * 24 * 30).optional(),
});

export const createApiKeySchema = z.object({
  title: z.string().min(2).max(120),
  scopes: z.array(scope).min(1).default(['*']),
  expiresAt: z.string().datetime().optional(),
});

export const bootstrapAdminSchema = z.object({
  adminToken: z.string().optional(),
  email,
  password,
  displayName: z.string().min(1).max(120).optional(),
  scopes: z.array(scope).default(['*']),
});

export const loginSchema = z.object({
  email,
  password: z.string().min(1),
  code: z.string().min(6).max(12).optional(),
  recoveryCode: z.string().min(8).max(32).optional(),
});

export const refreshSchema = z.object({
  refreshToken: z.string().min(20),
});

export const logoutSchema = refreshSchema;

export const changePasswordSchema = z.object({
  currentPassword: z.string().min(1),
  newPassword: password,
  revokeOtherSessions: z.boolean().default(true),
});

export const confirmTwoFactorSchema = z.object({
  code: z.string().min(6).max(12),
});

export const disableTwoFactorSchema = z.object({
  password: z.string().min(1),
  code: z.string().min(6).max(12).optional(),
  recoveryCode: z.string().min(8).max(32).optional(),
});

export const rotateRecoveryCodesSchema = z.object({
  password: z.string().min(1),
  code: z.string().min(6).max(12).optional(),
  recoveryCode: z.string().min(8).max(32).optional(),
});
