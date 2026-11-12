import type { CompatibilityTargetDefinition, UpgradePlanStep } from './types.js';

export const defaultCompatibilityTargets: CompatibilityTargetDefinition[] = [
  { code: 'runtime.node', kind: 'runtime', name: 'Node.js Active LTS', versionRange: '>=22 <25', required: true, status: 'supported', notes: 'Основной runtime для API, bot-runtime, worker и CLI.' },
  { code: 'package_manager.pnpm', kind: 'tooling', name: 'pnpm', versionRange: '>=9 <11', required: true, status: 'supported' },
  { code: 'database.postgresql', kind: 'database', name: 'PostgreSQL', versionRange: '>=17 <19', required: true, status: 'supported', notes: 'PostgreSQL 18 рекомендуется для новых production-инсталляций.' },
  { code: 'database.mysql', kind: 'database', name: 'MySQL', versionRange: '>=8.4 <9', required: false, status: 'experimental' },
  { code: 'database.mariadb', kind: 'database', name: 'MariaDB', versionRange: '>=10.11 <12', required: false, status: 'experimental' },
  { code: 'cache.redis', kind: 'cache', name: 'Redis', versionRange: '>=7 <9', required: true, status: 'supported' },
  { code: 'queue.redis', kind: 'queue', name: 'Redis/BullMQ queue', versionRange: '>=7 <9', required: true, status: 'supported' },
  { code: 'queue.postgresql', kind: 'queue', name: 'PostgreSQL Queue', versionRange: '>=17 <19', required: false, status: 'supported' },
  { code: 'queue.rabbitmq', kind: 'queue', name: 'RabbitMQ', versionRange: '>=3.13 <5', required: false, status: 'experimental' },
  { code: 'provider.telegram', kind: 'provider', name: 'Telegram Bot API', versionRange: 'Bot API compatible', required: true, status: 'supported' },
  { code: 'provider.vk', kind: 'provider', name: 'VK Callback/Long Poll API', versionRange: 'current', required: true, status: 'supported' },
  { code: 'provider.discord', kind: 'provider', name: 'Discord API / discord.js 14', versionRange: '^14', required: true, status: 'supported' },
  { code: 'deploy.docker_compose', kind: 'deployment', name: 'Docker Compose', versionRange: '>=2.20', required: true, status: 'supported' },
  { code: 'deploy.kubernetes', kind: 'deployment', name: 'Kubernetes', versionRange: '>=1.29', required: false, status: 'deprecated', notes: 'Не является blocker для 1.0.0; planned post-1.0.' },
];

export const defaultUpgradeSteps: UpgradePlanStep[] = [
  { code: 'preflight.version', title: 'Проверить текущую версию beta 3', phase: 'preflight', required: true, command: 'cat VERSION' },
  { code: 'preflight.doctor', title: 'Запустить doctor перед обновлением', phase: 'preflight', required: true, command: 'cajeer doctor' },
  { code: 'backup.create', title: 'Создать backup community/system data', phase: 'backup', required: true, command: 'cajeer backup create --community <id>' },
  { code: 'db.migrate', title: 'Применить Prisma migrations до 1.0.0-rc.5', phase: 'migrate', required: true, command: 'pnpm prisma:migrate' },
  { code: 'db.seed', title: 'Обновить seed permissions/default jobs', phase: 'migrate', required: true, command: 'pnpm db:seed' },
  { code: 'contracts.verify', title: 'Проверить API freeze', phase: 'verify', required: true, command: 'cajeer rc api-freeze:verify --version 1.0.0-rc.5' },
  { code: 'release.gates', title: 'Запустить release gates', phase: 'verify', required: true, command: 'cajeer release-gates run --version 1.0.0-rc.5 --channel rc' },
  { code: 'integration.smoke', title: 'Запустить MVP integration smoke', phase: 'verify', required: true, command: 'cajeer integration-tests run:suite mvp-smoke --version 1.0.0-rc.5 --channel rc' },
  { code: 'security.audit', title: 'Запустить security audit', phase: 'verify', required: true, command: 'cajeer security audit:run --version 1.0.0-rc.5 --channel rc' },
  { code: 'rollback.restore', title: 'Rollback через backup restore при blocker', phase: 'rollback', required: false, command: 'cajeer backup restore <backupId> --apply' },
];
