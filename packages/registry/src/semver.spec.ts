import { describe, expect, it } from 'vitest';
import { compareSemver, satisfiesSemverRange } from './semver.js';
import { selectUpdateCandidate } from './planner.js';

describe('registry semver', () => {
  it('compares stable and prerelease versions', () => {
    expect(compareSemver('1.0.0', '1.0.0-beta.1')).toBeGreaterThan(0);
    expect(compareSemver('1.2.0', '1.1.9')).toBeGreaterThan(0);
  });

  it('checks common ranges without external semver dependency', () => {
    expect(satisfiesSemverRange('0.10.0', '>=0.8.0 <1.0.0')).toBe(true);
    expect(satisfiesSemverRange('1.0.0', '^0.10.0')).toBe(false);
    expect(satisfiesSemverRange('0.10.5', '~0.10.0')).toBe(true);
  });

  it('selects newest compatible update candidate', () => {
    const result = selectUpdateCandidate({
      currentVersion: '0.9.0',
      engineApiVersion: '0.10.0',
      versions: [
        { version: '0.10.0', engine: { range: '>=0.10.0 <1.0.0' }, channel: 'stable', yanked: false, artifact: { url: 'file://a', sha256: 'a'.repeat(64), signatureAlgorithm: 'none' } },
        { version: '0.11.0', engine: { range: '>=2.0.0' }, channel: 'stable', yanked: false, artifact: { url: 'file://b', sha256: 'b'.repeat(64), signatureAlgorithm: 'none' } },
      ],
    });
    expect(result.available).toBe(true);
    expect(result.targetVersion?.version).toBe('0.10.0');
  });
});
