import { describe, expect, it } from 'vitest';
import { botScenarioDefinitionSchema, runBotScenario } from './scenario-runner.js';

describe('bot scenario runner', () => {
  it('validates scenario definition', () => {
    const parsed = botScenarioDefinitionSchema.parse({ steps: [{ type: 'command', command: 'help' }] });
    expect(parsed.defaultUserExternalId).toBe('dev-user');
    expect(parsed.steps[0]?.type).toBe('command');
  });

  it('runs dry-run scenario without DB writes', async () => {
    const result = await runBotScenario({ prisma: {}, communityId: 'community', accountId: 'account', dryRun: true, definition: { steps: [{ type: 'command', command: 'debug' }] } });
    expect(result.status).toBe('succeeded');
    expect(result.steps).toHaveLength(1);
    expect(result.steps[0]?.assertion).toMatchObject({ dryRun: true });
  });
});
