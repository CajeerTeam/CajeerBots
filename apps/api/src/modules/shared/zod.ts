import { BadRequestException } from '@nestjs/common';
import type { z } from 'zod';

export function parseBody<T extends z.ZodTypeAny>(schema: T, value: unknown): z.infer<T> {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new BadRequestException({
      code: 'VALIDATION_ERROR',
      issues: result.error.issues.map((issue) => ({ path: issue.path.join('.'), message: issue.message })),
    });
  }
  return result.data;
}

export function parseParam<T extends z.ZodTypeAny>(schema: T, value: unknown): z.infer<T> {
  return parseBody(schema, value);
}
