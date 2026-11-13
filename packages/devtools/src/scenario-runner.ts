import { z } from 'zod';
import { persistSimulatedProviderUpdate, type SimulatedProvider } from './provider-simulator.js';

export const scenarioStepSchema = z.object({
  name: z.string().optional(),
  type: z.enum(['message', 'command', 'callback', 'form', 'wait', 'assert_outbound']).default('message'),
  text: z.string().optional(),
  command: z.string().optional(),
  callbackCode: z.string().optional(),
  formCode: z.string().optional(),
  userExternalId: z.string().optional(),
  chatExternalId: z.string().optional(),
  payload: z.record(z.string(), z.unknown()).optional(),
  waitMs: z.number().int().min(0).max(30_000).optional(),
  expectOutboundTextContains: z.string().optional(),
});

export const botScenarioDefinitionSchema = z.object({
  provider: z.enum(['telegram', 'vk', 'discord']).optional(),
  accountId: z.string().optional(),
  defaultUserExternalId: z.string().default('dev-user'),
  defaultChatExternalId: z.string().optional(),
  variables: z.record(z.string(), z.unknown()).optional(),
  steps: z.array(scenarioStepSchema).min(1),
});

export type BotScenarioDefinition = z.infer<typeof botScenarioDefinitionSchema>;
export type BotScenarioStep = z.infer<typeof scenarioStepSchema>;

export interface RunBotScenarioInput {
  prisma: any;
  communityId: string;
  scenarioId?: string;
  scenarioCode?: string;
  definition: unknown;
  provider?: SimulatedProvider | null;
  accountId?: string | null;
  triggeredBy?: string;
  dryRun?: boolean;
}

export interface ScenarioStepResult {
  index: number;
  name?: string;
  type: string;
  status: 'passed' | 'failed' | 'skipped';
  inboundUpdateId?: string;
  assertion?: Record<string, unknown>;
  error?: string;
  durationMs: number;
}

export interface ScenarioRunResult {
  status: 'succeeded' | 'failed';
  steps: ScenarioStepResult[];
  error?: string;
  durationMs: number;
}

export async function runBotScenario(input: RunBotScenarioInput): Promise<ScenarioRunResult> {
  const started = Date.now();
  const definition = botScenarioDefinitionSchema.parse(input.definition);
  const provider = (input.provider ?? definition.provider ?? 'telegram') as SimulatedProvider;
  const accountId = input.accountId ?? definition.accountId;
  if (!accountId && !input.dryRun) throw new Error('Для запуска сценария нужен accountId');
  const stepResults: ScenarioStepResult[] = [];
  try {
    for (const [index, step] of definition.steps.entries()) {
      const stepStarted = Date.now();
      const result = await runScenarioStep(input.prisma, {
        communityId: input.communityId,
        accountId: accountId ?? 'dry-run-account',
        provider,
        step,
        defaultUserExternalId: definition.defaultUserExternalId,
        defaultChatExternalId: definition.defaultChatExternalId ?? definition.defaultUserExternalId,
        dryRun: Boolean(input.dryRun),
      });
      stepResults.push({ index, name: step.name, type: step.type, durationMs: Date.now() - stepStarted, ...result });
      if (result.status === 'failed') {
        return { status: 'failed', steps: stepResults, error: result.error, durationMs: Date.now() - started };
      }
    }
    return { status: 'succeeded', steps: stepResults, durationMs: Date.now() - started };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { status: 'failed', steps: stepResults, error: message, durationMs: Date.now() - started };
  }
}

async function runScenarioStep(prisma: any, context: {
  communityId: string;
  accountId: string;
  provider: SimulatedProvider;
  step: BotScenarioStep;
  defaultUserExternalId: string;
  defaultChatExternalId: string;
  dryRun: boolean;
}): Promise<Omit<ScenarioStepResult, 'index' | 'type' | 'durationMs' | 'name'>> {
  const step = context.step;
  if (step.type === 'wait') {
    await sleep(step.waitMs ?? 500);
    return { status: 'passed' };
  }
  if (step.type === 'assert_outbound') {
    return assertOutbound(prisma, context, step);
  }
  const userExternalId = step.userExternalId ?? context.defaultUserExternalId;
  const chatExternalId = step.chatExternalId ?? context.defaultChatExternalId;
  if (context.dryRun) {
    return { status: 'passed', assertion: { dryRun: true, provider: context.provider, userExternalId, chatExternalId } };
  }
  const inbound = await persistSimulatedProviderUpdate(prisma, {
    provider: context.provider,
    accountId: context.accountId,
    communityId: context.communityId,
    type: step.type,
    text: step.text,
    command: step.command,
    callbackCode: step.callbackCode,
    userExternalId,
    chatExternalId,
    payload: { ...(step.payload ?? {}), formCode: step.formCode },
  });
  if (step.expectOutboundTextContains) {
    const assertion = await findOutboundContaining(prisma, context.communityId, context.accountId, chatExternalId, step.expectOutboundTextContains, new Date(Date.now() - 30_000));
    if (!assertion.found) return { status: 'failed', inboundUpdateId: inbound.id, error: `Не найден outbound message содержащий: ${step.expectOutboundTextContains}`, assertion };
    return { status: 'passed', inboundUpdateId: inbound.id, assertion };
  }
  return { status: 'passed', inboundUpdateId: inbound.id };
}

async function assertOutbound(prisma: any, context: { communityId: string; accountId: string; step: BotScenarioStep; defaultChatExternalId: string }, step: BotScenarioStep): Promise<Omit<ScenarioStepResult, 'index' | 'type' | 'durationMs' | 'name'>> {
  const expected = step.expectOutboundTextContains ?? step.text;
  if (!expected) return { status: 'failed', error: 'assert_outbound требует expectOutboundTextContains или text' };
  const chatExternalId = step.chatExternalId ?? context.defaultChatExternalId;
  const assertion = await findOutboundContaining(prisma, context.communityId, context.accountId, chatExternalId, expected, new Date(Date.now() - 300_000));
  return assertion.found ? { status: 'passed', assertion } : { status: 'failed', error: `Не найден outbound message содержащий: ${expected}`, assertion };
}

async function findOutboundContaining(prisma: any, communityId: string, accountId: string, targetId: string, expected: string, since: Date): Promise<Record<string, unknown>> {
  const messages = await prisma.botOutboundMessage.findMany({
    where: { communityId, accountId, targetId, createdAt: { gte: since } },
    orderBy: { createdAt: 'desc' },
    take: 50,
  });
  const found = messages.find((message: any) => String(message.text ?? '').includes(expected));
  return { found: Boolean(found), expected, messageId: found?.id, checked: messages.length };
}

function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}
