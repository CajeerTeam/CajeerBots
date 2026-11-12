import { describe, expect, it } from 'vitest';
import { summarizeResults } from './evaluator.js';

it('summarizes failed gates as failed beta gate run', () => {
  const summary = summarizeResults('1.0.0-rc.2', 'beta', [
    { code: 'a', title: 'A', category: 'test', status: 'passed', severity: 'info', summary: {}, issues: [], durationMs: 1 },
    { code: 'b', title: 'B', category: 'test', status: 'failed', severity: 'blocker', summary: {}, issues: [{ code: 'b.fail', severity: 'blocker', message: 'broken' }], durationMs: 1 },
  ]);
  expect(summary.status).toBe('failed');
  expect(summary.blockers).toBe(1);
});

describe('release gates package', () => {
  it('exports stable beta version policy helpers through summary', () => {
    const summary = summarizeResults('1.0.0-rc.2', 'beta', []);
    expect(summary.version).toBe('1.0.0-rc.2');
    expect(summary.channel).toBe('beta');
  });
});
