import { describe, expect, it } from 'vitest';
import { createBackupBundle, decodeBackupBundle, encodeBackupBundle, verifyBackupBundle } from './backup-bundle.js';

describe('backup bundle', () => {
  it('encodes, decodes and verifies payload checksum', () => {
    const bundle = createBackupBundle({ scope: 'community', resourceId: 'c1', title: 'test', includedResources: ['settings'], payload: { version: '1.0.0-rc.1', settings: [{ key: 'x', value: true }] } });
    const decoded = decodeBackupBundle(encodeBackupBundle(bundle));
    expect(decoded.title).toBe('test');
    expect(verifyBackupBundle(decoded).valid).toBe(true);
  });
});
