import { commandMatches } from './commands.js';
import type { IncomingInteraction, InteractionHandler, InteractionResponse, PersistedInteractionCommand } from './types.js';

export class InteractionRouter {
  private readonly handlers = new Map<string, InteractionHandler>();
  private commands: PersistedInteractionCommand[] = [];

  register(name: string, handler: InteractionHandler): void {
    this.handlers.set(name, handler);
  }

  loadCommands(commands: PersistedInteractionCommand[]): void {
    this.commands = commands.filter((command) => command.isEnabled);
  }

  findCommand(interaction: IncomingInteraction): PersistedInteractionCommand | undefined {
    const name = interaction.name;
    if (!name) return undefined;
    return this.commands.find((command) => commandMatches(command, interaction.provider, name));
  }

  async dispatch(interaction: IncomingInteraction): Promise<InteractionResponse | void> {
    const persisted = this.findCommand(interaction);
    const handlerKey = persisted?.handler ?? interaction.name ?? interaction.type;
    const handler = this.handlers.get(handlerKey) ?? this.handlers.get(interaction.name ?? '') ?? this.handlers.get(interaction.type);
    if (handler) return handler({ ...interaction, payload: { ...interaction.payload, command: persisted } });
    if (persisted?.templateKey) return { templateKey: persisted.templateKey, variables: defaultVariables(interaction) };
    return undefined;
  }
}

function defaultVariables(interaction: IncomingInteraction): Record<string, string> {
  const communityId = String(interaction.payload.communityId ?? '');
  return { provider: interaction.provider, userId: interaction.userId, communityId, command: interaction.name ?? '' };
}
