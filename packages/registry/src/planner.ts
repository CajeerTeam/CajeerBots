import type { RegistryChannel, RegistryPackageVersion } from './schema.js';
import { compareSemver, isNewerVersion, satisfiesSemverRange } from './semver.js';

export interface UpdateCandidateInput {
  currentVersion: string;
  engineApiVersion: string;
  versions: readonly RegistryPackageVersion[];
  channel?: RegistryChannel;
  includePrerelease?: boolean;
}

export interface UpdateCandidateResult {
  available: boolean;
  currentVersion: string;
  targetVersion?: RegistryPackageVersion;
  skipped: Array<{ version: string; reason: string }>;
}

export function selectUpdateCandidate(input: UpdateCandidateInput): UpdateCandidateResult {
  const skipped: Array<{ version: string; reason: string }> = [];
  const ordered = [...input.versions].sort((a, b) => compareSemver(b.version, a.version));
  for (const version of ordered) {
    if (version.yanked) {
      skipped.push({ version: version.version, reason: 'yanked' });
      continue;
    }
    if (input.channel && version.channel !== input.channel) {
      skipped.push({ version: version.version, reason: `channel:${version.channel}` });
      continue;
    }
    if (!input.includePrerelease && version.version.includes('-')) {
      skipped.push({ version: version.version, reason: 'prerelease' });
      continue;
    }
    if (!isNewerVersion(version.version, input.currentVersion)) {
      skipped.push({ version: version.version, reason: 'not-newer' });
      continue;
    }
    if (!satisfiesSemverRange(input.engineApiVersion, version.engine.range)) {
      skipped.push({ version: version.version, reason: `engine:${version.engine.range}` });
      continue;
    }
    return { available: true, currentVersion: input.currentVersion, targetVersion: version, skipped };
  }
  return { available: false, currentVersion: input.currentVersion, skipped };
}

export function updateSteps(kind: 'core' | 'module' | 'plugin' | 'sdk', withSignature: boolean): string[] {
  const base = [
    'resolve-registry-source',
    'select-compatible-version',
    'download-artifact',
    'verify-sha256',
  ];
  if (withSignature) base.push('verify-signature');
  if (kind === 'core') base.push('backup-current-release', 'apply-core-artifact', 'run-migrations', 'restart-services');
  else base.push('install-package-version', 'apply-permissions-policy', 'write-update-history');
  return base;
}
