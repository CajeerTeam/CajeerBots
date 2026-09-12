import type { SystemdUnitOptions } from './types.js';

export function renderSystemdUnit(options: SystemdUnitOptions): string {
  const user = options.user ?? 'cajeerbots';
  const restartSec = options.restartSec ?? 5;
  const env = options.environmentFile ? `EnvironmentFile=${options.environmentFile}\n` : '';
  return `[Unit]\nDescription=${options.description}\nAfter=network-online.target postgresql.service redis.service\nWants=network-online.target\n\n[Service]\nType=simple\nUser=${user}\nWorkingDirectory=${options.workingDirectory}\n${env}ExecStart=${options.command}\nRestart=always\nRestartSec=${restartSec}\nNoNewPrivileges=true\nPrivateTmp=true\nProtectSystem=strict\nReadWritePaths=${options.workingDirectory} /var/lib/cajeerbots /var/log/cajeerbots\n\n[Install]\nWantedBy=multi-user.target\n`;
}

export function renderDefaultSystemdUnits(workdir = '/opt/cajeerbots', envFile = '/etc/cajeerbots/cajeerbots.env'): Record<string, string> {
  return {
    'cajeerbots-api.service': renderSystemdUnit({ name: 'cajeerbots-api', description: 'CajeerBots API', workingDirectory: workdir, environmentFile: envFile, command: 'node apps/api/dist/main.js' }),
    'cajeerbots-bot-runtime.service': renderSystemdUnit({ name: 'cajeerbots-bot-runtime', description: 'CajeerBots Bot Runtime', workingDirectory: workdir, environmentFile: envFile, command: 'node apps/bot-runtime/dist/main.js' }),
    'cajeerbots-worker.service': renderSystemdUnit({ name: 'cajeerbots-worker', description: 'CajeerBots Worker', workingDirectory: workdir, environmentFile: envFile, command: 'node apps/worker/dist/main.js' }),
  };
}
