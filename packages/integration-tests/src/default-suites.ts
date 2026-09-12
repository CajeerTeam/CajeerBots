export const defaultIntegrationCoverageAreas = [
  { code: 'api', title: 'REST API', requiredTargets: ['api'] },
  { code: 'security', title: 'Auth/RBAC/Security', requiredTargets: ['security'] },
  { code: 'bots', title: 'Telegram/VK/Discord Bot Runtime', requiredTargets: ['bot-runtime', 'telegram', 'vk', 'discord'] },
  { code: 'events', title: 'Event API / Outbox / Webhooks', requiredTargets: ['event-api', 'outbox'] },
  { code: 'extensions', title: 'Extension Runtime', requiredTargets: ['extensions'] },
  { code: 'ops', title: 'Deployment/Observability/Docs', requiredTargets: ['deployment', 'observability', 'docs'] },
] as const;

export const defaultIntegrationSuites = [
  {
    code: 'mvp-smoke',
    title: 'MVP smoke: API, DB, Event API, Outbox',
    category: 'mvp',
    description: 'Базовый smoke-набор для beta 2: проверяет API health, БД, Event API и outbox-очереди.',
    cases: [
      {
        code: 'api-db-event-smoke',
        title: 'API/DB/Event smoke',
        target: 'api',
        kind: 'multi_step',
        priority: 10,
        definition: {
          steps: [
            { kind: 'api_health', code: 'api.health', request: { path: '/api/health' }, expect: { status: [200, 401, 403] } },
            { kind: 'database_count', code: 'db.communities', request: { model: 'community' }, expect: { minCount: 1 } },
            { kind: 'event_publish', code: 'event.publish', request: { topic: 'integration.smoke', type: 'integration.smoke.completed', payload: { source: 'beta2' } }, expect: { status: 'created' } },
            { kind: 'outbox_probe', code: 'outbox.dead', request: {}, expect: { maxDead: 0 } }
          ]
        },
        expected: { status: 'passed' }
      },
      {
        code: 'release-gates-summary',
        title: 'Release gates summary probe',
        target: 'release-gates',
        kind: 'multi_step',
        priority: 20,
        definition: { steps: [{ kind: 'release_gate_summary', code: 'release.summary', request: {}, expect: { maxOpenBlockers: 0 } }] },
        expected: { status: 'passed' }
      }
    ]
  },
  {
    code: 'bot-runtime-smoke',
    title: 'Bot Runtime scenario smoke',
    category: 'bot-runtime',
    description: 'Проверяет наличие активных bot scenarios и возможность dry-run выполнения сценария.',
    cases: [
      {
        code: 'help-scenario-dry-run',
        title: 'Help scenario dry-run',
        target: 'bot-runtime',
        kind: 'bot_scenario',
        priority: 30,
        definition: { steps: [{ kind: 'bot_scenario', code: 'scenario.help', request: { scenarioCode: 'system.help-smoke', dryRun: true }, expect: { status: ['passed', 'skipped'] } }] },
        expected: { status: 'passed' }
      }
    ]
  }
] as const;
