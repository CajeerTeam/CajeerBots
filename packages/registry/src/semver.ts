export interface ParsedSemver {
  major: number;
  minor: number;
  patch: number;
  prerelease?: string;
}

export function parseSemver(version: string): ParsedSemver {
  const normalized = version.trim().replace(/^v/, '');
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$/.exec(normalized);
  if (!match) throw new Error(`Некорректная SemVer версия: ${version}`);
  const parsed: ParsedSemver = { major: Number(match[1]), minor: Number(match[2]), patch: Number(match[3]) };
  if (match[4]) parsed.prerelease = match[4];
  return parsed;
}

export function compareSemver(a: string, b: string): number {
  const left = parseSemver(a);
  const right = parseSemver(b);
  for (const key of ['major', 'minor', 'patch'] as const) {
    const diff = left[key] - right[key];
    if (diff !== 0) return diff;
  }
  if (left.prerelease && !right.prerelease) return -1;
  if (!left.prerelease && right.prerelease) return 1;
  if (!left.prerelease && !right.prerelease) return 0;
  return String(left.prerelease).localeCompare(String(right.prerelease));
}

export function isNewerVersion(candidate: string, current: string): boolean {
  return compareSemver(candidate, current) > 0;
}

export function satisfiesSemverRange(version: string, range: string): boolean {
  const trimmed = range.trim();
  if (trimmed === '' || trimmed === '*' || trimmed === 'x') return true;
  if (trimmed.includes('||')) return trimmed.split('||').some((part) => satisfiesSemverRange(version, part));
  const parts = trimmed.split(/\s+/).filter(Boolean);
  if (parts.length > 1) return parts.every((part) => satisfiesSemverRange(version, part));
  const part = parts[0] ?? trimmed;
  if (part.startsWith('^')) return satisfiesCaret(version, part.slice(1));
  if (part.startsWith('~')) return satisfiesTilde(version, part.slice(1));
  for (const op of ['>=', '<=', '>', '<', '='] as const) {
    if (part.startsWith(op)) {
      const target = part.slice(op.length);
      const cmp = compareSemver(version, target);
      if (op === '>=') return cmp >= 0;
      if (op === '<=') return cmp <= 0;
      if (op === '>') return cmp > 0;
      if (op === '<') return cmp < 0;
      return cmp === 0;
    }
  }
  return compareSemver(version, part) === 0;
}

function satisfiesCaret(version: string, base: string): boolean {
  const parsed = parseSemver(base);
  const upper = parsed.major > 0 ? `${parsed.major + 1}.0.0` : parsed.minor > 0 ? `0.${parsed.minor + 1}.0` : `0.0.${parsed.patch + 1}`;
  return compareSemver(version, base) >= 0 && compareSemver(version, upper) < 0;
}

function satisfiesTilde(version: string, base: string): boolean {
  const parsed = parseSemver(base);
  const upper = `${parsed.major}.${parsed.minor + 1}.0`;
  return compareSemver(version, base) >= 0 && compareSemver(version, upper) < 0;
}
