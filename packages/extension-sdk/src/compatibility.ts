export interface CompatibilityResult {
  compatible: boolean;
  reasons: string[];
}

interface Version {
  major: number;
  minor: number;
  patch: number;
}

export function checkEngineCompatibility(engineRange: string, currentApiVersion: string): CompatibilityResult {
  const current = parseVersion(currentApiVersion);
  if (!current) return { compatible: false, reasons: [`Некорректная версия API ядра: ${currentApiVersion}`] };

  const constraints = engineRange.split(/\s+/).map((item) => item.trim()).filter(Boolean);
  const reasons: string[] = [];
  for (const constraint of constraints) {
    if (!matchesConstraint(constraint, current)) reasons.push(`Не выполнено ограничение ${constraint} для API ${currentApiVersion}`);
  }
  return { compatible: reasons.length === 0, reasons };
}

function matchesConstraint(constraint: string, current: Version): boolean {
  if (constraint === '*' || constraint === 'x') return true;
  if (constraint.startsWith('^')) {
    const base = parseVersion(constraint.slice(1));
    if (!base) return false;
    return current.major === base.major && compareVersion(current, base) >= 0;
  }
  if (constraint.startsWith('~')) {
    const base = parseVersion(constraint.slice(1));
    if (!base) return false;
    return current.major === base.major && current.minor === base.minor && compareVersion(current, base) >= 0;
  }
  const operator = constraint.match(/^(>=|<=|>|<|=)?(.+)$/);
  if (!operator) return false;
  const expected = parseVersion(operator[2] ?? '');
  if (!expected) return false;
  const compared = compareVersion(current, expected);
  switch (operator[1] ?? '=') {
    case '>=': return compared >= 0;
    case '<=': return compared <= 0;
    case '>': return compared > 0;
    case '<': return compared < 0;
    case '=': return compared === 0;
    default: return false;
  }
}

function parseVersion(value: string): Version | null {
  const match = value.trim().match(/^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/);
  if (!match) return null;
  return { major: Number(match[1]), minor: Number(match[2]), patch: Number(match[3]) };
}

function compareVersion(a: Version, b: Version): number {
  return a.major - b.major || a.minor - b.minor || a.patch - b.patch;
}
