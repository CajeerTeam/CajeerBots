import type { IncomingInteraction } from '@cajeerbots/interaction-core';

export interface BotScenarioStep {
  name: string;
  interaction: IncomingInteraction;
  expect?: Record<string, unknown>;
}

export interface BotScenario {
  title: string;
  steps: BotScenarioStep[];
}

export async function runBotScenario(scenario: BotScenario): Promise<void> {
  for (const step of scenario.steps) {
    if (!step.interaction.userId) {
      throw new Error(`Scenario step has no userId: ${step.name}`);
    }
  }
}
