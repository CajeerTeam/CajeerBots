import { z } from 'zod';

// Unlike z.unknown(), this output cannot be undefined. Zod 3 therefore
// infers required object keys correctly, while still accepting JSON null.
export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() => z.union([
  z.string(),
  z.number().finite(),
  z.boolean(),
  z.null(),
  z.array(jsonValueSchema),
  z.record(z.string(), jsonValueSchema),
]));

export const jsonRecordSchema = z.record(z.string(), jsonValueSchema);
