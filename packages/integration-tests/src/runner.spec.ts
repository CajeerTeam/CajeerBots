import { describe, expect, it } from 'vitest';
import { assertExpectation } from './assertions.js';

describe('integration test assertions', () => {
  it('passes allowed status and count boundaries', () => {
    expect(assertExpectation({ status: 200, count: 2 }, { status: [200, 204], minCount: 1 }).passed).toBe(true);
  });

  it('fails dead queue threshold', () => {
    const result = assertExpectation({ status: 'ok', dead: 2 }, { maxDead: 0 });
    expect(result.passed).toBe(false);
    expect(result.message).toContain('dead=2');
  });
});
