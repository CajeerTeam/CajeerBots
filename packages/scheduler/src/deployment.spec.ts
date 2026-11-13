import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { checkDockerComposeProduction, type DeploymentCheckResult } from '@cajeerbots/deployment';

describe('scheduler deployment package integration', () => {
  it('resolves the built package and returns typed checks for existing and missing files', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cajeer-scheduler-deployment-'));
    try {
      const missing: DeploymentCheckResult[] = await checkDockerComposeProduction(root);
      expect(missing.find((check) => check.code === 'file.docker-compose.prod.yml.exists')?.status).toBe('fail');
      await writeFile(join(root, 'docker-compose.prod.yml'), [
        'services:', '  api:', '  bot-runtime:', '  worker:', '  postgres:', '  redis:',
        '    healthcheck: {}', '    user: 1000', '',
      ].join('\n'));
      await writeFile(join(root, '.env.production'), 'NODE_ENV=production\n');
      const checks: DeploymentCheckResult[] = await checkDockerComposeProduction(root);
      expect(checks).toHaveLength(9);
      expect(checks.every((check) => check.status === 'pass')).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
