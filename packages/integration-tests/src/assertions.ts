export interface AssertionResult {
  passed: boolean;
  details: Record<string, unknown>;
  message?: string;
}

export function assertExpectation(response: Record<string, unknown>, expect: Record<string, unknown> = {}): AssertionResult {
  const failures: string[] = [];
  if (expect.status !== undefined) {
    const actual = response.status;
    const allowed = Array.isArray(expect.status) ? expect.status : [expect.status];
    if (!allowed.includes(actual)) failures.push(`status=${String(actual)} не входит в ${allowed.join(',')}`);
  }
  if (expect.minCount !== undefined && Number(response.count ?? 0) < Number(expect.minCount)) failures.push(`count=${String(response.count)} меньше ${String(expect.minCount)}`);
  if (expect.maxDead !== undefined && Number(response.dead ?? 0) > Number(expect.maxDead)) failures.push(`dead=${String(response.dead)} больше ${String(expect.maxDead)}`);
  if (expect.maxOpenBlockers !== undefined && Number(response.openBlockers ?? 0) > Number(expect.maxOpenBlockers)) failures.push(`openBlockers=${String(response.openBlockers)} больше ${String(expect.maxOpenBlockers)}`);
  if (expect.bodyContains !== undefined && !String(response.body ?? '').includes(String(expect.bodyContains))) failures.push(`body не содержит ${String(expect.bodyContains)}`);
  if (expect.exists === true && response.exists !== true) failures.push('ожидался существующий объект');
  if (expect.equals !== undefined && JSON.stringify(response.value) !== JSON.stringify(expect.equals)) failures.push('value не равен expected.equals');
  return { passed: failures.length === 0, details: { expected: expect, actual: response, failures }, message: failures.join('; ') || undefined };
}
