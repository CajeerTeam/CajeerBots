export interface ConversationStep {
  id: string;
  messageKey?: string;
  text?: string;
  input?: 'none' | 'text' | 'number' | 'boolean' | 'choice';
  variable?: string;
  choices?: Array<{ label: string; value: string }>;
  next?: string;
  complete?: boolean;
}

export interface ConversationDefinition {
  id: string;
  title?: string;
  steps: ConversationStep[];
}

export interface ConversationRuntimeState {
  flow: string;
  step: string;
  data: Record<string, unknown>;
}

export function firstStep(definition: ConversationDefinition): ConversationStep | undefined {
  return definition.steps[0];
}

export function findStep(definition: ConversationDefinition, stepId: string): ConversationStep | undefined {
  return definition.steps.find((step) => step.id === stepId);
}

export function advanceConversation(definition: ConversationDefinition, state: ConversationRuntimeState, input?: unknown): { state: ConversationRuntimeState; step?: ConversationStep; completed: boolean } {
  const current = findStep(definition, state.step) ?? firstStep(definition);
  if (!current) return { state, completed: true };
  const data = { ...state.data };
  if (current.variable && input !== undefined) data[current.variable] = input;
  const nextId = current.next;
  if (!nextId || current.complete) return { state: { ...state, data }, completed: true };
  const next = findStep(definition, nextId);
  return { state: { flow: state.flow, step: nextId, data }, step: next, completed: !next };
}
