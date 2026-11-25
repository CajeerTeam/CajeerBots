import { afterEach, describe, expect, it, vi } from 'vitest';
import { workspaceTarget, workspaceText, workspaceKeyAllowed, workspaceMarker } from './workspace-delivery.js';
afterEach(() => vi.unstubAllEnvs());
describe('Workspace channel boundary', () => {
  it('requires exact key, community and valid channel', () => {
    vi.stubEnv('WORKSPACE_BOT_TARGETS', JSON.stringify({ key: { communityId: 'c1', accountId: 'a1', provider: 'telegram', channelId: '-100123456789' } }));
    expect(workspaceTarget('key', 'c1')).toEqual({ communityId: 'c1', accountId: 'a1', provider: 'telegram', channelId: '-100123456789' });
    expect(workspaceTarget('key', 'c2')).toBeNull();
    expect(workspaceTarget('other', 'c1')).toBeNull();
    expect(workspaceTarget('__proto__', 'c1')).toBeNull();
    expect(workspaceKeyAllowed(['workspace:messages', 'community:c1'], 'c1')).toBe(true);
    expect(workspaceKeyAllowed(['workspace:messages', '*', 'community:c1'], 'c1')).toBe(false);
    expect(workspaceKeyAllowed(['workspace:messages', 'community:c2'], 'c1')).toBe(false);
    expect(workspaceMarker('key', 'rev')).toBe('workspace:key:rev');
  });
  it('bounds text and prohibits control characters', () => {
    expect(workspaceText('Привет\nмир')).toBe(true);
    expect(workspaceText('a'.repeat(2001))).toBe(false);
    expect(workspaceText('a\0b')).toBe(false);
    expect(workspaceText('   ')).toBe(false);
    expect(workspaceText('🙂'.repeat(2000))).toBe(true);
  });
});
