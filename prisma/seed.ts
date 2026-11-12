import { PrismaClient, type Prisma } from './generated/client/index.js';

const db = new PrismaClient();

const permissions = [
  ['system:read', 'Чтение состояния системы, diagnostics и metrics'],
  ['audit:read', 'Чтение audit log'],
  ['communities:read', 'Чтение communities'],
  ['communities:write', 'Создание и изменение communities'],
  ['users:read', 'Чтение пользователей community'],
  ['users:write', 'Создание, изменение пользователей и назначение ролей'],
  ['roles:read', 'Чтение ролей и permissions'],
  ['roles:write', 'Создание ролей и изменение permissions'],
  ['settings:read', 'Чтение настроек community'],
  ['settings:write', 'Изменение настроек community'],
  ['bot_accounts:read', 'Чтение bot accounts'],
  ['bot_accounts:write', 'Создание и изменение bot accounts'],
  ['bot_runtime:read', 'Чтение runtime status, inbound updates и outbound messages ботов'],
  ['bot_runtime:write', 'Повторная отправка outbound messages и runtime-действия bot-runtime'],
  ['messages:read', 'Чтение message templates'],
  ['messages:write', 'Создание, изменение и рендер message templates'],
  ['message_core:read', 'Чтение локализации, button layouts, forms и render logs Message Core'],
  ['message_core:write', 'Управление локализацией, button layouts, forms и submissions Message Core'],
  ['interactions:read', 'Чтение команд, меню, callback actions, conversations и interaction events'],
  ['interactions:write', 'Создание и изменение команд, меню, callback actions и conversations'],
  ['extensions:read', 'Чтение установленных расширений'],
  ['extensions:install', 'Установка расширений'],
  ['extensions:write', 'Включение, отключение и изменение расширений'],
  ['extensions:permissions', 'Выдача и отзыв permission grants для расширений'],
  ['extensions:runtime', 'Постановка, просмотр и повторный запуск runtime events расширений'],
  ['extensions:storage', 'Чтение и изменение key/value storage расширений'],
  ['extensions:config', 'Изменение конфигурации расширений и просмотр истории'],
  ['webhooks:read', 'Чтение webhook endpoints'],
  ['webhooks:write', 'Управление webhook endpoints и signed events'],
  ['outbox:read', 'Чтение outbox и deliveries'],
  ['outbox:write', 'Повторная доставка outbox events'],
  ['event_api:read', 'Чтение Event API topics, journal, subscriptions, deliveries и dead-letter'],
  ['event_api:write', 'Управление Event API topics, subscriptions и dead-letter'],
  ['event_api:publish', 'Публикация durable domain/integration events через Event API'],
  ['event_api:replay', 'Replay событий и повторная постановка Event API deliveries'],
  ['auth:tokens:manage', 'Создание и отзыв API keys/JWT tokens'],
  ['auth:accounts:manage', 'Создание admin accounts, смена паролей и управление доступом администраторов'],
  ['auth:sessions:manage', 'Просмотр и отзыв admin sessions'],
  ['auth:2fa:manage', 'Настройка TOTP 2FA и recovery codes'],
  ['security:read', 'Чтение security hardening status и rate-limit violations'],
  ['security_audit:read', 'Чтение security audit policies, runs, findings и risk snapshots'],
  ['security_audit:write', 'Создание и изменение security audit policies, закрытие/принятие findings'],
  ['security_audit:run', 'Запуск security beta audit checks и создание risk snapshots'],
  ['security_audit:report', 'Создание и чтение security beta reports перед RC'],
  ['secrets:read', 'Чтение метаданных encrypted secrets'],
  ['secrets:write', 'Создание, обновление, ротация и удаление encrypted secrets'],
  ['secrets:reveal', 'Расшифровка значений encrypted secrets'],
  ['config:read', 'Чтение runtime-конфигурации и результата config validation'],
  ['import_export:read', 'Экспорт данных community'],
  ['import_export:write', 'Импорт данных community'],
  ['registry:read', 'Чтение registry sources, packages и versions'],
  ['registry:write', 'Создание и изменение registry sources'],
  ['registry:sync', 'Синхронизация registry manifest'],
  ['updates:read', 'Чтение update plans/history'],
  ['updates:plan', 'Создание update plans'],
  ['updates:apply', 'Применение update plans'],
  ['scheduler:read', 'Чтение scheduled jobs, run history, queue summary и maintenance snapshots'],
  ['scheduler:write', 'Создание, изменение, включение и отключение scheduled jobs'],
  ['scheduler:run', 'Ручной запуск scheduled jobs и maintenance tasks'],
  ['devtools:read', 'Чтение debug sessions, debug events, bot scenarios и scenario runs'],
  ['devtools:write', 'Создание debug sessions/events и управление bot scenarios'],
  ['devtools:run', 'Запуск bot scenarios и provider-update simulator для dev/test диагностики'],
  ['storage:read', 'Чтение storage objects и метаданных local/S3 хранилища'],
  ['storage:write', 'Удаление storage objects и управление lifecycle файлов'],
  ['backups:read', 'Чтение backup artifacts и history restore'],
  ['backups:write', 'Создание backup artifacts для community'],
  ['backups:restore', 'Dry-run/apply restore backup artifacts'],
  ['observability:read', 'Чтение metric samples, probes, alert rules, alert events и incidents'],
  ['observability:write', 'Сбор observability snapshots, управление probes и alert rules'],
  ['observability:manage', 'Ручное разрешение incidents и управление production diagnostics policy'],
  ['deployment:read', 'Чтение deployment targets, checks и release artifacts'],
  ['deployment:write', 'Генерация production env, Nginx/systemd конфигураций и управление targets'],
  ['deployment:run', 'Запуск deployment checks и ведение deployment run journal'],
  ['deployment:release', 'Создание release manifest и управление release artifacts'],
  ['contracts:read', 'Чтение OpenAPI snapshots, contract checks, SDK generation runs и API deprecations'],
  ['contracts:write', 'Создание OpenAPI snapshots, baseline snapshots и управление API deprecations'],
  ['contracts:check', 'Запуск contract-first checks и breaking-change detection'],
  ['contracts:sdk', 'Генерация SDK operation map/checklist из OpenAPI snapshots'],
  ['docs:read', 'Чтение GitFlic Wiki collections, pages, snapshots, checks и exports'],
  ['docs:write', 'Создание и изменение documentation collections'],
  ['docs:sync', 'Синхронизация wiki markdown-страниц в БД'],
  ['docs:check', 'Проверка документации: links, anchors, coverage'],
  ['docs:export', 'Создание documentation export bundles'],
  ['release_gates:read', 'Чтение release freezes, quality gates, gate runs, blockers и beta reports'],
  ['release_gates:write', 'Создание и изменение release freezes, quality gates и blockers'],
  ['release_gates:run', 'Запуск release quality gates и beta readiness checks'],
  ['release_gates:report', 'Создание release candidate reports и feature-freeze summaries'],
  ['integration_tests:read', 'Чтение integration test suites, cases, runs, step results и coverage reports'],
  ['integration_tests:write', 'Создание и изменение integration test suites/cases и default MVP regression наборов'],
  ['integration_tests:run', 'Запуск и постановка в очередь integration tests, MVP smoke и regression проверок'],
  ['integration_tests:report', 'Создание и чтение integration coverage/release test отчетов'],
  ['release_candidate:read', 'Чтение RC API freeze, compatibility matrix, upgrade plans и readiness runs'],
  ['release_candidate:write', 'Создание RC API freeze, compatibility targets и upgrade plans'],
  ['release_candidate:verify', 'Запуск API freeze verification, compatibility matrix, upgrade rehearsal и RC readiness checks'],
  ['release_candidate:apply', 'Применение RC upgrade plan и release-candidate операций'],
] as const;

async function main(prisma: Prisma.TransactionClient): Promise<void> {
  await prisma.$queryRaw`SELECT pg_advisory_xact_lock(27012026)::text AS lock`;
  const community = await prisma.community.upsert({
    where: { slug: 'default' },
    create: { slug: 'default', title: 'Default Community', language: 'ru' },
    update: {},
  });

  for (const [scope, description] of permissions) {
    await prisma.permission.upsert({ where: { scope }, create: { scope, description }, update: {} });
  }

  const existingRoles = await prisma.role.findMany({ where: { communityId: community.id, code: { in: ['owner', 'admin'] } }, select: { code: true } });
  const existingCodes = new Set(existingRoles.map((role) => role.code));
  const owner = await prisma.role.upsert({
    where: { communityId_code: { communityId: community.id, code: 'owner' } },
    create: { communityId: community.id, code: 'owner', title: 'Владелец', priority: 0 },
    update: {},
  });

  const admin = await prisma.role.upsert({
    where: { communityId_code: { communityId: community.id, code: 'admin' } },
    create: { communityId: community.id, code: 'admin', title: 'Администратор', priority: 10 },
    update: {},
  });

  const allPermissions = await prisma.permission.findMany();
  for (const role of [owner, admin]) {
    if (existingCodes.has(role.code)) continue;
    for (const permission of allPermissions) {
      await prisma.rolePermission.upsert({
        where: { roleId_permissionId: { roleId: role.id, permissionId: permission.id } },
        create: { roleId: role.id, permissionId: permission.id },
        update: {},
      });
    }
  }

  await prisma.setting.upsert({
    where: { communityId_key: { communityId: community.id, key: 'bot.locale' } },
    create: { communityId: community.id, key: 'bot.locale', value: 'ru' },
    update: {},
  });

  await prisma.messageTemplate.upsert({
    where: { key_locale: { key: 'system.help', locale: 'ru' } },
    create: { key: 'system.help', locale: 'ru', body: 'CajeerBots: команды /cajeer_status, /cajeer_help, /cajeer_settings.' },
    update: {},
  });

  await prisma.messageTemplate.upsert({
    where: { key_locale: { key: 'system.status', locale: 'ru' } },
    create: { key: 'system.status', locale: 'ru', body: 'Статус: {{status}}. Версия: {{version}}.' },
    update: {},
  });



  await prisma.messageTemplate.upsert({
    where: { key_locale: { key: 'system.settings', locale: 'ru' } },
    create: { key: 'system.settings', locale: 'ru', body: 'Настройки сообщества {{communityId}}: язык {{locale}}.' },
    update: {},
  });



  await prisma.localeCatalog.upsert({
    where: { communityId_namespace_locale: { communityId: community.id, namespace: 'system', locale: 'ru' } },
    create: {
      communityId: community.id,
      namespace: 'system',
      locale: 'ru',
      messages: {
        'system.welcome': 'Добро пожаловать в CajeerBots. Используйте /cajeer_help для списка команд.',
        'system.status.ok': 'CajeerBots работает. Версия: {{version}}.',
        'forms.submitted': 'Форма {{formCode}} принята.',
      },
    },
    update: {},
  });

  await prisma.messageButtonLayout.upsert({
    where: { communityId_code_provider_locale: { communityId: community.id, code: 'system.main', provider: null, locale: 'ru' } },
    create: {
      communityId: community.id,
      code: 'system.main',
      locale: 'ru',
      rows: [[
        { id: 'status', label: 'Статус', action: 'status' },
        { id: 'help', label: 'Помощь', action: 'help' },
      ]],
    },
    update: {},
  });

  await prisma.messageFormDefinition.upsert({
    where: { communityId_code_locale: { communityId: community.id, code: 'support.request', locale: 'ru' } },
    create: {
      communityId: community.id,
      code: 'support.request',
      title: 'Обращение в поддержку',
      locale: 'ru',
      fields: [
        { name: 'topic', label: 'Тема', type: 'text', required: true, min: 3, max: 120 },
        { name: 'message', label: 'Сообщение', type: 'text', required: true, min: 5, max: 2000 },
      ],
      successTemplateKey: 'forms.support.success',
    },
    update: {},
  });

  await prisma.interactionCommand.upsert({
    where: { communityId_provider_name: { communityId: community.id, provider: null, name: 'start' } },
    create: { communityId: community.id, name: 'start', description: 'Стартовое сообщение CajeerBots', aliases: ['cajeer'], handler: 'template', templateKey: 'system.help' },
    update: {},
  });

  await prisma.interactionCommand.upsert({
    where: { communityId_provider_name: { communityId: community.id, provider: null, name: 'status' } },
    create: { communityId: community.id, name: 'status', description: 'Проверка состояния bot-runtime', aliases: ['cajeer_status'], handler: 'status', templateKey: 'system.status' },
    update: {},
  });

  await prisma.interactionCommand.upsert({
    where: { communityId_provider_name: { communityId: community.id, provider: null, name: 'help' } },
    create: { communityId: community.id, name: 'help', description: 'Список доступных команд', aliases: ['cajeer_help'], handler: 'template', templateKey: 'system.help' },
    update: {},
  });



  await prisma.interactionCommand.upsert({
    where: { communityId_provider_name: { communityId: community.id, provider: null, name: 'debug' } },
    create: { communityId: community.id, name: 'debug', description: 'DevTools debug toolbar без внешней веб-панели', aliases: ['cajeer_debug'], handler: 'debug', templateKey: null, scopes: ['devtools:read'] },
    update: {},
  });

  await prisma.interactionMenu.upsert({
    where: { communityId_code_locale: { communityId: community.id, code: 'main', locale: 'ru' } },
    create: {
      communityId: community.id,
      code: 'main',
      title: 'Главное меню',
      locale: 'ru',
      templateKey: 'system.help',
      buttons: [[{ id: 'status', label: 'Статус', action: 'command.status' }, { id: 'help', label: 'Помощь', action: 'command.help' }]],
    },
    update: {},
  });



  await prisma.eventTopic.upsert({
    where: { name: 'system.audit' },
    create: { name: 'system.audit', description: 'Системные audit/domain события CajeerBots', retentionDays: 90 },
    update: {},
  });

  await prisma.eventTopic.upsert({
    where: { name: 'bot.interaction' },
    create: { name: 'bot.interaction', description: 'События взаимодействий Telegram/VK/Discord', retentionDays: 30 },
    update: {},
  });



  await prisma.registrySource.upsert({
    where: { name: 'cajeer-official' },
    create: {
      name: 'cajeer-official',
      url: 'https://gitflic.ru/project/cajeer/cajeerbots-registry/blob/raw?file=registry.json',
      type: 'gitflic_registry',
      isActive: true,
      syncStatus: 'never',
    },
    update: {},
  });


  const scheduledJobs = [
    {
      code: 'maintenance.cleanup_expired_idempotency',
      title: 'Очистка просроченных idempotency keys',
      type: 'built_in',
      handler: 'maintenance.cleanup_expired_idempotency',
      schedule: 'cron:*/30 * * * *',
      payload: {},
    },
    {
      code: 'maintenance.retry_stale_processing',
      title: 'Возврат зависших processing-задач в retry',
      type: 'built_in',
      handler: 'maintenance.retry_stale_processing',
      schedule: 'cron:*/10 * * * *',
      payload: { olderThanSeconds: 900 },
    },
    {
      code: 'maintenance.event_retention',
      title: 'Очистка Event API journal по retentionDays',
      type: 'built_in',
      handler: 'maintenance.event_retention',
      schedule: 'cron:15 3 * * *',
      payload: {},
    },
    {
      code: 'maintenance.diagnostics_snapshot',
      title: 'Снимок queue/cache/storage diagnostics',
      type: 'built_in',
      handler: 'maintenance.diagnostics_snapshot',
      schedule: 'cron:*/15 * * * *',
      payload: {},
    },
    {
      code: 'maintenance.backup_retention',
      title: 'Очистка удалённых storage objects после retention window',
      type: 'built_in',
      handler: 'maintenance.backup_retention',
      schedule: 'cron:20 4 * * *',
      payload: { deletedObjectRetentionDays: 7 },
    },
    {
      code: 'maintenance.observability_collect',
      title: 'Сбор observability metrics и alert evaluation',
      type: 'built_in',
      handler: 'maintenance.observability_collect',
      schedule: 'cron:*/5 * * * *',
      payload: { includeDiagnostics: true, evaluateAlerts: true, runProbes: true },
    },
    {
      code: 'maintenance.cleanup_render_logs',
      title: 'Очистка старых message render logs',
      type: 'built_in',
      handler: 'maintenance.cleanup_render_logs',
      schedule: 'cron:40 2 * * *',
      payload: { retentionDays: 14 },
    },
  ];

  for (const job of scheduledJobs) {
    await prisma.scheduledJob.upsert({
      where: { code: job.code },
      create: { ...job, nextRunAt: new Date(Date.now() + 60_000), maxAttempts: 3, timeoutSeconds: 300 },
      update: {},
    });
  }



  await prisma.botScenario.upsert({
    where: { communityId_code: { communityId: community.id, code: 'system.help-smoke' } },
    create: {
      communityId: community.id,
      code: 'system.help-smoke',
      title: 'Smoke-проверка команды help',
      provider: null,
      definition: {
        defaultUserExternalId: 'dev-user',
        steps: [
          { name: 'Вызов /help', type: 'command', command: 'help', chatExternalId: 'dev-chat' },
          { name: 'Проверка outbound help', type: 'assert_outbound', chatExternalId: 'dev-chat', expectOutboundTextContains: 'CajeerBots' },
        ],
      },
      isActive: true,
    },
    update: {},
  });

  await prisma.debugSession.upsert({
    where: { id: 'seed-devtools-session' },
    create: { id: 'seed-devtools-session', communityId: community.id, title: 'Seed DevTools session', status: 'ended', actorId: 'seed', actorType: 'system', endedAt: new Date(), metadata: { version: '1.0.0-rc.2' } },
    update: {},
  });

  await prisma.eventSubscription.upsert({
    where: { name: 'system-log-all-events' },
    create: { name: 'system-log-all-events', topicPattern: '*', targetType: 'log', targetRef: 'event-api.log', isActive: true, maxAttempts: 3 },
    update: {},
  });


  await prisma.observabilityProbe.upsert({
    where: { code: 'database.default' },
    create: { code: 'database.default', title: 'PostgreSQL primary', type: 'database', intervalSeconds: 60, timeoutMs: 2000, isEnabled: true, nextCheckAt: new Date() },
    update: {},
  });

  await prisma.observabilityProbe.upsert({
    where: { code: 'cache.redis' },
    create: { code: 'cache.redis', title: 'Redis cache/queue', type: 'redis_tcp', intervalSeconds: 60, timeoutMs: 2000, isEnabled: true, nextCheckAt: new Date() },
    update: {},
  });

  await prisma.observabilityAlertRule.upsert({
    where: { code: 'outbox.dead.nonzero' },
    create: { code: 'outbox.dead.nonzero', title: 'Есть dead-сообщения outbox', metric: 'outbox.dead', operator: 'gte', threshold: 1, severity: 'critical', cooldownSeconds: 900, isEnabled: true },
    update: {},
  });



  await prisma.deploymentTarget.upsert({
    where: { code: 'docker.production' },
    create: {
      code: 'docker.production',
      title: 'Docker Compose production',
      kind: 'docker_compose',
      environment: 'production',
      baseUrl: 'https://bots.example.cajeer.ru',
      composeFiles: ['docker-compose.prod.yml'],
      envFile: '.env.production',
      status: 'unknown',
      metadata: { nginx: 'docker/nginx/cajeerbots.conf', profile: 'prod' },
    },
    update: {},
  });

  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.deployment_readiness' },
    create: { code: 'maintenance.deployment_readiness', title: 'Проверка production deployment readiness', type: 'built_in', handler: 'maintenance.deployment_readiness', schedule: 'cron:0 6 * * *', payload: { targetCode: 'docker.production' }, nextRunAt: new Date(Date.now() + 60_000), maxAttempts: 2, timeoutSeconds: 120 },
    update: {},
  });


  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.contracts_check' },
    create: {
      code: 'maintenance.contracts_check',
      title: 'Contract-first OpenAPI/SDK governance check',
      type: 'built_in',
      handler: 'maintenance.contracts_check',
      schedule: 'cron:30 6 * * *',
      payload: { sourcePath: 'contracts/openapi/cajeerbots.openapi.yaml', sdkSourcePath: 'packages/sdk/src/client.ts', minSdkCoveragePercent: 50 },
      nextRunAt: new Date(Date.now() + 60_000),
      maxAttempts: 2,
      timeoutSeconds: 120,
    },
    update: {},
  });




  await prisma.documentationCollection.upsert({
    where: { code: 'gitflic-wiki' },
    create: {
      code: 'gitflic-wiki',
      title: 'GitFlic Wiki',
      locale: 'ru',
      sourceType: 'local_wiki',
      sourcePath: 'wiki',
      sourceUrl: 'https://gitflic.ru/project/cajeer/cajeerbots/wiki',
      metadata: { product: 'CajeerBots', version: '1.0.0-rc.2', docsLanguage: 'ru' },
    },
    update: {},
  });

  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.docs_sync' },
    create: {
      code: 'maintenance.docs_sync',
      title: 'Синхронизация GitFlic Wiki documentation pages',
      type: 'built_in',
      handler: 'maintenance.docs_sync',
      schedule: 'cron:15 6 * * *',
      payload: { collectionCode: 'gitflic-wiki', rootDir: 'wiki', includeDrafts: false },
      nextRunAt: new Date(Date.now() + 60_000),
      maxAttempts: 2,
      timeoutSeconds: 180,
    },
    update: {},
  });

  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.docs_check' },
    create: {
      code: 'maintenance.docs_check',
      title: 'Проверка GitFlic Wiki на битые ссылки и anchors',
      type: 'built_in',
      handler: 'maintenance.docs_check',
      schedule: 'cron:45 6 * * *',
      payload: { collectionCode: 'gitflic-wiki', rootDir: 'wiki', includeDrafts: false },
      nextRunAt: new Date(Date.now() + 90_000),
      maxAttempts: 2,
      timeoutSeconds: 180,
    },
    update: {},
  });

  await prisma.releaseFreeze.upsert({
    where: { version_channel: { version: '1.0.0-rc.2', channel: 'rc' } },
    create: {
      version: '1.0.0-rc.2',
      channel: 'rc',
      status: 'locked',
      reason: 'Beta 1 feature freeze: разрешены bugfix/test/docs/security изменения, новые крупные подсистемы блокируются до RC.',
      scope: { features: 'frozen', allowedChanges: ['bugfix', 'test', 'docs', 'security'], milestone: 'MVP feature freeze' },
      lockedBy: 'seed',
    },
    update: {},
  });

  const releaseQualityGates = [
    ['project.required_files', 'Обязательные product/release файлы присутствуют', 'project', 'blocker', { files: ['package.json', 'VERSION', 'CHANGELOG.md', 'README.md', 'SECURITY.md', 'LICENSE', 'NOTICE', 'prisma/schema.prisma', 'prisma/seed.ts', 'contracts/openapi/cajeerbots.openapi.yaml', 'docker-compose.yml', 'docker-compose.prod.yml', '.env.example', '.env.production.example'] }],
    ['project.version_aligned', 'Версия синхронизирована в VERSION/package.json/workspaces/OpenAPI', 'project', 'blocker', { packageGlobs: ['apps/*/package.json', 'packages/*/package.json'], openApiPath: 'contracts/openapi/cajeerbots.openapi.yaml' }],
    ['database.migrations_present', 'Для RC1 есть Prisma migration и seed permissions', 'database', 'blocker', { migrationPrefix: '20260617190100_rc1_100', requiredSeedStrings: ['release_candidate:read', 'release_candidate:write', 'release_candidate:verify', 'release_candidate:apply'] }],
    ['release.no_blocker_markers', 'В runtime-коде нет blocker-маркеров TODO/stub/not implemented', 'quality', 'blocker', { roots: ['apps', 'packages'], includeExtensions: ['.ts', '.tsx', '.js', '.mjs', '.prisma'], patterns: ['TODO', 'FIXME', 'not implemented', 'skeleton', 'not_checked', 'заглуш'], exclude: ['node_modules', 'dist', '.git', 'CHANGED_FILES', 'packages/release-gates/src/default-gates.ts', 'default-gates.ts'] }],
    ['openapi.parseable', 'OpenAPI парсится и содержит operationId', 'api', 'error', { sourcePath: 'contracts/openapi/cajeerbots.openapi.yaml', minOperations: 25 }],
    ['docs.wiki_present', 'Русская GitFlic Wiki документация присутствует и проверяется', 'docs', 'warning', { rootDir: 'wiki', minPages: 10 }],
    ['deployment.production_files', 'Production deployment артефакты готовы', 'deployment', 'error', { files: ['docker-compose.prod.yml', 'docker/api.Dockerfile', 'docker/bot-runtime.Dockerfile', 'docker/worker.Dockerfile', 'docker/nginx/cajeerbots.conf'] }],
  ] as const;

  for (const [code, title, category, severity, config] of releaseQualityGates) {
    await prisma.releaseQualityGate.upsert({
      where: { code },
      create: { code, title, category, severity, isEnabled: true, config },
      update: {},
    });
  }

  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.release_gates' },
    create: {
      code: 'maintenance.release_gates',
      title: 'RC1 release gates и MVP readiness checks',
      type: 'built_in',
      handler: 'maintenance.release_gates',
      schedule: 'cron:30 7 * * *',
      payload: { version: '1.0.0-rc.2', channel: 'rc', rootDir: '.' },
      nextRunAt: new Date(Date.now() + 120_000),
      maxAttempts: 2,
      timeoutSeconds: 300,
    },
    update: {},
  });



  const integrationSuite = await prisma.integrationTestSuite.upsert({
    where: { code: 'mvp-smoke' },
    create: {
      code: 'mvp-smoke',
      title: 'MVP smoke: API, DB, Event API, Outbox',
      category: 'mvp',
      description: 'Базовый integration smoke-набор для RC1.',
      metadata: { seededBy: '1.0.0-rc.2' },
    },
    update: {},
  });

  await prisma.integrationTestCase.upsert({
    where: { suiteId_code: { suiteId: integrationSuite.id, code: 'api-db-event-smoke' } },
    create: {
      suiteId: integrationSuite.id,
      code: 'api-db-event-smoke',
      title: 'API/DB/Event smoke',
      target: 'api',
      kind: 'multi_step',
      priority: 10,
      definition: {
        steps: [
          { kind: 'api_health', code: 'api.health', request: { path: '/api/health' }, expect: { status: [200, 401, 403] } },
          { kind: 'database_count', code: 'db.communities', request: { model: 'community' }, expect: { minCount: 1 } },
          { kind: 'event_publish', code: 'event.publish', request: { topic: 'integration.smoke', type: 'integration.smoke.completed', payload: { source: 'seed' } }, expect: { status: 'created' } },
          { kind: 'outbox_probe', code: 'outbox.dead', request: {}, expect: { maxDead: 0 } },
        ],
      },
      expected: { status: 'passed' },
    },
    update: {},
  });

  await prisma.integrationTestCase.upsert({
    where: { suiteId_code: { suiteId: integrationSuite.id, code: 'release-gates-summary' } },
    create: {
      suiteId: integrationSuite.id,
      code: 'release-gates-summary',
      title: 'Release gates summary probe',
      target: 'release-gates',
      kind: 'multi_step',
      priority: 20,
      definition: { steps: [{ kind: 'release_gate_summary', code: 'release.summary', request: {}, expect: { maxOpenBlockers: 0 } }] },
      expected: { status: 'passed' },
    },
    update: {},
  });

  const botIntegrationSuite = await prisma.integrationTestSuite.upsert({
    where: { code: 'bot-runtime-smoke' },
    create: { code: 'bot-runtime-smoke', title: 'Bot Runtime scenario smoke', category: 'bot-runtime', description: 'Dry-run проверка bot scenario без внешних provider API.', metadata: { seededBy: '1.0.0-rc.2' } },
    update: {},
  });

  await prisma.integrationTestCase.upsert({
    where: { suiteId_code: { suiteId: botIntegrationSuite.id, code: 'help-scenario-dry-run' } },
    create: {
      suiteId: botIntegrationSuite.id,
      code: 'help-scenario-dry-run',
      title: 'Help scenario dry-run',
      target: 'bot-runtime',
      kind: 'bot_scenario',
      priority: 30,
      definition: { steps: [{ kind: 'bot_scenario', code: 'scenario.help', request: { scenarioCode: 'system.help-smoke', dryRun: true }, expect: { status: ['passed', 'skipped'] } }] },
      expected: { status: 'passed' },
    },
    update: {},
  });

  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.integration_smoke' },
    create: {
      code: 'maintenance.integration_smoke',
      title: 'RC1 MVP integration smoke suite',
      type: 'built_in',
      handler: 'maintenance.integration_smoke',
      schedule: 'cron:0 8 * * *',
      payload: { suiteCode: 'mvp-smoke', version: '1.0.0-rc.2', channel: 'rc', dryRun: true },
      nextRunAt: new Date(Date.now() + 150_000),
      maxAttempts: 2,
      timeoutSeconds: 300,
    },
    update: {},
  });



  const securityAuditPolicies = [
    ['auth.admin_2fa_required', 'Все активные admin accounts должны иметь подтвержденную 2FA', 'auth', 'critical', { requireForAllAdmins: true }],
    ['auth.sessions_not_stale', 'Активные refresh sessions не должны быть слишком старыми', 'auth', 'warning', { maxAgeDays: 30 }],
    ['tokens.expired_api_keys_revoked', 'Истекшие API keys должны быть отозваны', 'tokens', 'error', {}],
    ['tokens.api_keys_scoped', 'Активные API keys должны иметь непустой набор scopes', 'tokens', 'error', {}],
    ['secrets.rotation_due', 'Secrets должны ротироваться по политике возраста', 'secrets', 'warning', { maxAgeDays: 90 }],
    ['webhooks.secret_required', 'Активные webhook endpoints должны использовать secretRef', 'webhooks', 'error', {}],
    ['rate_limit.violations_threshold', 'Rate-limit violations не должны превышать RC-порог', 'rate_limit', 'warning', { windowHours: 24, maxViolations: 100 }],
    ['release.no_open_security_blockers', 'Не должно быть открытых security/release blockers', 'release', 'critical', {}],
  ] as const;

  for (const [code, title, category, severity, config] of securityAuditPolicies) {
    await prisma.securityAuditPolicy.upsert({
      where: { code },
      create: { code, title, category, severity, isEnabled: true, config },
      update: {},
    });
  }

  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.security_audit' },
    create: {
      code: 'maintenance.security_audit',
      title: 'RC1 security audit и risk snapshot',
      type: 'built_in',
      handler: 'maintenance.security_audit',
      schedule: 'cron:30 8 * * *',
      payload: { version: '1.0.0-rc.2', channel: 'rc', persist: true },
      nextRunAt: new Date(Date.now() + 180_000),
      maxAttempts: 2,
      timeoutSeconds: 300,
    },
    update: {},
  });



  const rcCompatibilityTargets = [
    ['runtime.node', 'runtime', 'Node.js Active LTS', '>=22 <25', true, 'supported', 'Основной runtime для API, bot-runtime, worker и CLI'],
    ['package_manager.pnpm', 'tooling', 'pnpm', '>=9 <11', true, 'supported', null],
    ['database.postgresql', 'database', 'PostgreSQL', '>=17 <19', true, 'supported', 'PostgreSQL 18 рекомендуется для новых production-инсталляций'],
    ['database.mysql', 'database', 'MySQL', '>=8.4 <9', false, 'experimental', null],
    ['database.mariadb', 'database', 'MariaDB', '>=10.11 <12', false, 'experimental', null],
    ['cache.redis', 'cache', 'Redis', '>=7 <9', true, 'supported', null],
    ['queue.redis', 'queue', 'Redis/BullMQ queue', '>=7 <9', true, 'supported', null],
    ['queue.postgresql', 'queue', 'PostgreSQL Queue', '>=17 <19', false, 'supported', null],
    ['queue.rabbitmq', 'queue', 'RabbitMQ', '>=3.13 <5', false, 'experimental', null],
    ['provider.telegram', 'provider', 'Telegram Bot API', 'Bot API compatible', true, 'supported', null],
    ['provider.vk', 'provider', 'VK Callback/Long Poll API', 'current', true, 'supported', null],
    ['provider.discord', 'provider', 'Discord API / discord.js 14', '^14', true, 'supported', null],
    ['deploy.docker_compose', 'deployment', 'Docker Compose', '>=2.20', true, 'supported', null],
    ['deploy.kubernetes', 'deployment', 'Kubernetes', '>=1.29', false, 'deprecated', 'Не является blocker для 1.0.0; planned post-1.0'],
  ] as const;

  for (const [code, kind, name, versionRange, required, status, notes] of rcCompatibilityTargets) {
    await prisma.releaseCompatibilityTarget.upsert({
      where: { code },
      create: { code, kind, name, versionRange, required, status, notes, checks: [] },
      update: {},
    });
  }

  await prisma.releaseFreeze.upsert({
    where: { version_channel: { version: '1.0.0-rc.2', channel: 'rc' } },
    create: {
      version: '1.0.0-rc.2',
      channel: 'rc',
      status: 'locked',
      reason: 'RC1 API/feature freeze: допускаются только bugfix, docs, tests, security fixes.',
      scope: { features: 'frozen', allowedChanges: ['bugfix', 'docs', 'tests', 'security'], api: 'frozen', extensionApi: 'frozen' },
      lockedBy: 'seed',
    },
    update: {},
  });

  await prisma.scheduledJob.upsert({
    where: { code: 'maintenance.rc_readiness' },
    create: {
      code: 'maintenance.rc_readiness',
      title: 'RC1 readiness: API freeze, compatibility, blockers, security, integration smoke',
      type: 'built_in',
      handler: 'maintenance.rc_readiness',
      schedule: 'cron:0 9 * * *',
      payload: { version: '1.0.0-rc.2', channel: 'rc', rootDir: '.', persist: true },
      nextRunAt: new Date(Date.now() + 210_000),
      maxAttempts: 2,
      timeoutSeconds: 360,
    },
    update: {},
  });

  await prisma.observabilityAlertRule.upsert({
    where: { code: 'event.deadletters.nonzero' },
    create: { code: 'event.deadletters.nonzero', title: 'Есть open dead-letter события Event API', metric: 'event_api.dead_letters.open', operator: 'gte', threshold: 1, severity: 'warning', cooldownSeconds: 900, isEnabled: true },
    update: {},
  });

    console.log(`Seed готов: community=${community.slug}, roles=owner/admin, permissions=${allPermissions.length}, scheduler_jobs=${scheduledJobs.length + 4}, devtools_scenarios=1, observability=enabled, contracts=enabled, docs=enabled, release_gates=enabled, integration_tests=enabled, security_audit=enabled, rc_readiness=enabled`);
}

try {
  await db.$transaction(main, { maxWait: 30_000, timeout: 120_000 });
} finally {
  await db.$disconnect();
}
