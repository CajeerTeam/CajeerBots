import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
export function startService(name, env = process.env, watch = false) {
  const child = spawn(process.execPath, [...(watch ? ['--watch'] : []), `apps/${name}/dist/main.js`], { cwd: root, env, stdio: 'inherit' });
  const exited = new Promise((resolve) => {
    child.once('exit', (code, signal) => resolve({ code, signal }));
    child.once('error', (error) => resolve({ code: 1, error: error.message }));
  });
  return { name, child, exited };
}
export async function stopServices(services) {
  for (const { child } of services) if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
  await Promise.all(services.map(async ({ child, exited, name }) => {
    let timer;
    try {
      const result = await Promise.race([exited, new Promise((resolve) => { timer = setTimeout(() => resolve(null), 35_000); })]);
      if (result === null) { child.kill('SIGKILL'); await exited; throw new Error(`${name} did not shut down gracefully`); }
      if (result.code !== 0 && result.signal !== 'SIGTERM') throw new Error(`${name} shutdown: ${JSON.stringify(result)}`);
    } finally { clearTimeout(timer); }
  }));
}
export async function waitHealth(service, url, timeoutMs = 90_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (service.child.exitCode !== null || service.child.signalCode !== null) throw new Error(`${service.name} exited before readiness`);
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(3000) });
      if (response.ok) return await response.json();
    } catch { /* retry until deadline */ }
    if (Date.now() >= deadline) throw new Error(`${service.name} did not become ready: ${url}`);
    await delay(500);
  }
}
