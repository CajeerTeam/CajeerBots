import { z } from 'zod';

export type BotFormFieldType = 'text' | 'number' | 'boolean' | 'select' | 'multiselect';

export interface BotFormField {
  name: string;
  label: string;
  type: BotFormFieldType;
  required?: boolean;
  options?: Array<{ label: string; value: string }>;
  min?: number;
  max?: number;
  pattern?: string;
}

export interface BotForm<TSchema extends z.ZodTypeAny = z.ZodTypeAny> {
  id: string;
  title: string;
  fields: BotFormField[];
  schema?: TSchema;
}

export interface FormValidationResult {
  ok: boolean;
  normalized: Record<string, unknown>;
  errors: Array<{ field: string; message: string }>;
}

export function validateBotForm(fields: BotFormField[], payload: Record<string, unknown>): FormValidationResult {
  const normalized: Record<string, unknown> = {};
  const errors: Array<{ field: string; message: string }> = [];

  for (const field of fields) {
    const raw = payload[field.name];
    if ((raw === undefined || raw === null || raw === '') && field.required) {
      errors.push({ field: field.name, message: 'Обязательное поле' });
      continue;
    }
    if (raw === undefined || raw === null || raw === '') continue;

    if (field.type === 'text') {
      const value = String(raw);
      if (field.min !== undefined && value.length < field.min) errors.push({ field: field.name, message: `Минимальная длина: ${field.min}` });
      if (field.max !== undefined && value.length > field.max) errors.push({ field: field.name, message: `Максимальная длина: ${field.max}` });
      if (field.pattern && !new RegExp(field.pattern).test(value)) errors.push({ field: field.name, message: 'Значение не соответствует формату' });
      normalized[field.name] = value;
      continue;
    }

    if (field.type === 'number') {
      const value = Number(raw);
      if (!Number.isFinite(value)) errors.push({ field: field.name, message: 'Нужно число' });
      if (field.min !== undefined && value < field.min) errors.push({ field: field.name, message: `Минимум: ${field.min}` });
      if (field.max !== undefined && value > field.max) errors.push({ field: field.name, message: `Максимум: ${field.max}` });
      normalized[field.name] = value;
      continue;
    }

    if (field.type === 'boolean') {
      normalized[field.name] = raw === true || raw === 'true' || raw === '1' || raw === 1 || raw === 'да';
      continue;
    }

    const allowed = new Set((field.options ?? []).map((option) => option.value));
    if (field.type === 'select') {
      const value = String(raw);
      if (allowed.size > 0 && !allowed.has(value)) errors.push({ field: field.name, message: 'Недопустимое значение' });
      normalized[field.name] = value;
      continue;
    }

    const values = Array.isArray(raw) ? raw.map(String) : String(raw).split(',').map((item) => item.trim()).filter(Boolean);
    const invalid = values.filter((value) => allowed.size > 0 && !allowed.has(value));
    if (invalid.length > 0) errors.push({ field: field.name, message: `Недопустимые значения: ${invalid.join(', ')}` });
    normalized[field.name] = values;
  }

  return { ok: errors.length === 0, normalized, errors };
}
