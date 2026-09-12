export class CajeerError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly meta?: Record<string, unknown>,
  ) {
    super(message);
    this.name = 'CajeerError';
  }
}

export class PermissionDeniedError extends CajeerError {
  constructor(scope: string) {
    super(`Недостаточно прав: ${scope}`, 'PERMISSION_DENIED', { scope });
  }
}
