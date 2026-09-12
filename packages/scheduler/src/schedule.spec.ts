import { describe, expect, it } from 'vitest';
import { nextRunAt, parseDurationSeconds } from './schedule.js';

describe('scheduler schedule parser', () => {
  it('parses interval durations', () => {
    expect(parseDurationSeconds('5m')).toBe(300);
    expect(parseDurationSeconds('2h')).toBe(7200);
  });

  it('computes cron next run', () => {
    const next = nextRunAt('cron:*/15 * * * *', new Date('2026-06-17T10:01:00Z'));
    expect(next?.getUTCMinutes()).toBe(15);
  });
});
