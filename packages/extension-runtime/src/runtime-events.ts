export function hookMatches(pattern: string, hook: string): boolean {
  if (pattern === '*' || pattern === hook) return true;
  if (pattern.endsWith('.*')) return hook.startsWith(pattern.slice(0, -1));
  return false;
}

export function normalizeHookList(hooks: string[]): string[] {
  return Array.from(new Set(hooks.map((hook) => hook.trim()).filter(Boolean)));
}

export function missingPermissions(required: string[], granted: string[]): string[] {
  const grantSet = new Set(granted);
  return Array.from(new Set(required.filter((permission) => !grantSet.has(permission))));
}
