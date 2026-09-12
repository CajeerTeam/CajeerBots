export function hasScope(scopes: readonly string[], required: string): boolean {
  return scopes.includes(required) || scopes.includes('*');
}
