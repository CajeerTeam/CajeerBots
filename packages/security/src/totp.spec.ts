import { describe, expect, it } from 'vitest';
import { createOtpAuthUri, createTotpSecret, generateTotp, verifyTotp } from './index.js';

describe('totp helpers', () => {
  it('generates and verifies TOTP token', () => {
    const secret = createTotpSecret();
    const timestamp = Date.UTC(2026, 0, 1, 0, 0, 0);
    const token = generateTotp(secret, { timestamp });
    expect(verifyTotp(token, secret, { timestamp, window: 0 })).toBe(true);
    expect(verifyTotp('000000', secret, { timestamp, window: 0 })).toBe(false);
  });

  it('creates otpauth uri', () => {
    const secret = createTotpSecret();
    const uri = createOtpAuthUri({ issuer: 'CajeerBots', accountName: 'admin@example.com', secret });
    expect(uri).toContain('otpauth://totp/');
    expect(uri).toContain(secret);
  });
});
