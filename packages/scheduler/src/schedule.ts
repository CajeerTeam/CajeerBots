export interface ScheduleDefinition {
  kind: 'interval' | 'cron' | 'once' | 'manual';
  expression?: string;
  intervalSeconds?: number;
  runAt?: string;
  timezone?: string;
}

export function parseSchedule(raw: string | null | undefined): ScheduleDefinition {
  const value = String(raw ?? 'manual').trim();
  if (!value || value === 'manual') return { kind: 'manual' };
  if (value === 'once') return { kind: 'once' };
  if (value.startsWith('interval:')) return { kind: 'interval', intervalSeconds: parseDurationSeconds(value.slice('interval:'.length)) };
  if (value.startsWith('every:')) return { kind: 'interval', intervalSeconds: parseDurationSeconds(value.slice('every:'.length)) };
  if (value.startsWith('at:')) return { kind: 'once', runAt: value.slice('at:'.length) };
  if (value.startsWith('cron:')) return { kind: 'cron', expression: value.slice('cron:'.length).trim() };
  if (/^\d+[smhd]$/.test(value)) return { kind: 'interval', intervalSeconds: parseDurationSeconds(value) };
  return { kind: 'cron', expression: value };
}

export function nextRunAt(raw: string | null | undefined, from = new Date()): Date | null {
  const schedule = parseSchedule(raw);
  if (schedule.kind === 'manual') return null;
  if (schedule.kind === 'once') {
    const at = schedule.runAt ? new Date(schedule.runAt) : null;
    return at && Number.isFinite(at.getTime()) && at.getTime() > from.getTime() ? at : null;
  }
  if (schedule.kind === 'interval') return new Date(from.getTime() + Math.max(schedule.intervalSeconds ?? 60, 1) * 1000);
  if (schedule.kind === 'cron') return nextCronRun(schedule.expression ?? '* * * * *', from);
  return null;
}

export function parseDurationSeconds(value: string): number {
  const normalized = value.trim().toLowerCase();
  const match = normalized.match(/^(\d+)(s|m|h|d)?$/);
  if (!match) throw new Error(`Некорректный interval schedule: ${value}`);
  const amount = Number(match[1]);
  const unit = match[2] ?? 's';
  const multipliers: Record<string, number> = { s: 1, m: 60, h: 3600, d: 86400 };
  return amount * multipliers[unit];
}

export function nextCronRun(expression: string, from = new Date()): Date {
  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) throw new Error(`Поддерживается только 5-field cron: ${expression}`);
  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;
  const start = new Date(from.getTime() + 60_000);
  start.setSeconds(0, 0);
  for (let i = 0; i < 527_040; i += 1) {
    const candidate = new Date(start.getTime() + i * 60_000);
    if (
      matchesCronField(minute, candidate.getMinutes(), 0, 59) &&
      matchesCronField(hour, candidate.getHours(), 0, 23) &&
      matchesCronField(dayOfMonth, candidate.getDate(), 1, 31) &&
      matchesCronField(month, candidate.getMonth() + 1, 1, 12) &&
      matchesCronField(dayOfWeek, candidate.getDay(), 0, 7)
    ) return candidate;
  }
  throw new Error(`Не удалось вычислить следующий запуск cron за год: ${expression}`);
}

export function matchesCronField(field: string, value: number, min: number, max: number): boolean {
  if (field === '*') return true;
  return field.split(',').some((part) => {
    if (part.includes('/')) {
      const [base, stepRaw] = part.split('/');
      const step = Number(stepRaw);
      if (!Number.isInteger(step) || step <= 0) return false;
      if (base === '*') return (value - min) % step === 0;
      if (base.includes('-')) {
        const [from, to] = base.split('-').map(Number);
        return value >= from && value <= to && (value - from) % step === 0;
      }
    }
    if (part.includes('-')) {
      const [from, to] = part.split('-').map(Number);
      return value >= from && value <= to;
    }
    const numeric = Number(part);
    return value === numeric || (max === 7 && numeric === 7 && value === 0);
  });
}
