export type Result<T, E extends Error = Error> =
  | { ok: true; value: T }
  | { ok: false; error: E };

export function ok<T>(value: T): Result<T> {
  return { ok: true, value };
}

export function fail<E extends Error>(error: E): Result<never, E> {
  return { ok: false, error };
}
