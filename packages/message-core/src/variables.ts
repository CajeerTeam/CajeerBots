export type RenderVariableValue = string | number | boolean | null | undefined | Date | Record<string, unknown> | unknown[];

export interface RenderVariablesOptions {
  missing?: 'empty' | 'keep' | 'throw';
  transforms?: Record<string, (value: unknown) => string>;
}

export function renderVariables(
  template: string,
  variables: Record<string, RenderVariableValue>,
  options: RenderVariablesOptions = {},
): string {
  const missing = options.missing ?? 'empty';
  return template.replaceAll(/\{\{\s*([\w.-]+)(?:\|([\w.-]+))?\s*\}\}/g, (match, path: string, transform?: string) => {
    const value = readPath(variables, path);
    if (value === undefined) {
      if (missing === 'throw') throw new Error(`Переменная не найдена: ${path}`);
      return missing === 'keep' ? match : '';
    }
    if (transform && options.transforms?.[transform]) return options.transforms[transform](value);
    return stringifyVariable(value);
  });
}

export function extractVariables(template: string): string[] {
  const keys = new Set<string>();
  for (const match of template.matchAll(/\{\{\s*([\w.-]+)(?:\|[\w.-]+)?\s*\}\}/g)) keys.add(match[1]);
  return [...keys];
}

export function readPath(source: Record<string, unknown>, path: string): unknown {
  return path.split('.').reduce<unknown>((value, part) => {
    if (value && typeof value === 'object' && part in value) return (value as Record<string, unknown>)[part];
    return undefined;
  }, source);
}

export function stringifyVariable(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return String(value);
  return JSON.stringify(value);
}
