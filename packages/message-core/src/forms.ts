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
    if (['__proto__', 'constructor', 'prototype'].includes(field.name)) { errors.push({ field: field.name, message: 'Недопустимое имя поля' }); continue; }
    const raw = Object.hasOwn(payload, field.name) ? payload[field.name] : undefined;
    if ((raw === undefined || raw === null || raw === '') && field.required) {
      errors.push({ field: field.name, message: 'Обязательное поле' });
      continue;
    }
    if (raw === undefined || raw === null || raw === '') continue;

    if (field.type === 'text') {
      const value = String(raw);
      if (field.min !== undefined && value.length < field.min) errors.push({ field: field.name, message: `Минимальная длина: ${field.min}` });
      if (field.max !== undefined && value.length > field.max) errors.push({ field: field.name, message: `Максимальная длина: ${field.max}` });
      if (field.pattern) { try { if (!new RegExp(field.pattern).test(value)) errors.push({ field: field.name, message: 'Значение не соответствует формату' }); } catch { errors.push({ field: field.name, message: 'Некорректный формат поля' }); } }
      normalized[field.name] = value;
      continue;
    }

    if (field.type === 'number') {
      const value = typeof raw === 'number' || (typeof raw === 'string' && raw.trim()) ? Number(raw) : NaN;
      if (!Number.isFinite(value)) errors.push({ field: field.name, message: 'Нужно число' });
      if (field.min !== undefined && value < field.min) errors.push({ field: field.name, message: `Минимум: ${field.min}` });
      if (field.max !== undefined && value > field.max) errors.push({ field: field.name, message: `Максимум: ${field.max}` });
      if (Number.isFinite(value)) normalized[field.name] = value;
      continue;
    }

    if (field.type === 'boolean') {
      const value = String(raw).toLowerCase();
      if (!['true', 'false', '1', '0', 'да', 'нет', 'yes', 'no'].includes(value)) errors.push({ field: field.name, message: 'Нужно логическое значение (да/нет)' });
      else normalized[field.name] = ['true', '1', 'да', 'yes'].includes(value);
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
    if (field.required && !values.length) errors.push({ field: field.name, message: 'Обязательное поле' });
    const invalid = values.filter((value) => allowed.size > 0 && !allowed.has(value));
    if (invalid.length > 0) errors.push({ field: field.name, message: `Недопустимые значения: ${invalid.join(', ')}` });
    normalized[field.name] = values;
  }

  return { ok: errors.length === 0, normalized, errors };
}
