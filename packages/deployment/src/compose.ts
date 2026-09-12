import { access, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { DeploymentCheckResult } from './types.js';

export async function checkDockerComposeProduction(rootDir: string, files = ['docker-compose.prod.yml', '.env.production']): Promise<DeploymentCheckResult[]> {
  const checks: DeploymentCheckResult[] = [];
  for (const file of files) {
    try {
      await access(join(rootDir, file));
      checks.push({ code: `file.${file}.exists`, status: 'pass', severity: 'info', message: `${file} найден` });
    } catch {
      checks.push({ code: `file.${file}.exists`, status: file.endsWith('.env.production') ? 'warn' : 'fail', severity: file.endsWith('.env.production') ? 'warning' : 'critical', message: `${file} не найден` });
    }
  }

  try {
    const compose = await readFile(join(rootDir, 'docker-compose.prod.yml'), 'utf8');
    for (const service of ['api', 'bot-runtime', 'worker', 'postgres', 'redis']) {
      checks.push({ code: `compose.service.${service}`, status: compose.includes(`${service}:`) ? 'pass' : 'fail', severity: compose.includes(`${service}:`) ? 'info' : 'critical', message: compose.includes(`${service}:`) ? `service ${service} описан` : `service ${service} отсутствует` });
    }
    checks.push({ code: 'compose.healthchecks', status: compose.includes('healthcheck:') ? 'pass' : 'warn', severity: compose.includes('healthcheck:') ? 'info' : 'warning', message: compose.includes('healthcheck:') ? 'healthcheck присутствует' : 'healthcheck не найден' });
    checks.push({ code: 'compose.non_root', status: compose.includes('user:') ? 'pass' : 'warn', severity: compose.includes('user:') ? 'info' : 'warning', message: compose.includes('user:') ? 'задан user для runtime services' : 'runtime services могут запускаться от root' });
  } catch {
    // File existence check already captured this.
  }
  return checks;
}
