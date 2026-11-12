import { describe, expect, it } from 'vitest';
import { createRecoveryCodes, hashPassword, hashRecoveryCode, verifyPassword } from './index.js';

describe('password security helpers', () => {
  it('hashes and verifies password with pepper', () => {
    const hash = hashPassword('very-strong-password', { pepper: 'pepper' });
    expect(hash).not.toContain('very-strong-password');
    expect(verifyPassword('very-strong-password', hash, { pepper: 'pepper' })).toBe(true);
    expect(verifyPassword('very-strong-password', hash, { pepper: 'other' })).toBe(false);
  });

  it('creates hashable recovery codes', () => {
    const [code] = createRecoveryCodes(1);
    if (!code) throw new Error("recovery code missing");
    expect(code).toContain('-');
    expect(hashRecoveryCode(code, 'pepper')).toBe(hashRecoveryCode(code.toLowerCase().replace('-', ' - '), 'pepper'));
  });
});
