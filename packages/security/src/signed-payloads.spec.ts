import { describe, expect, it } from 'vitest';
import { signPayload, verifyPayloadSignature } from './signed-payloads.js';

describe('signed payloads', () => {
  it('verifies signature', () => {
    const signature = signPayload('{"ok":true}', 'secret');
    expect(verifyPayloadSignature('{"ok":true}', 'secret', signature)).toBe(true);
  });
});
