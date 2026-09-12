import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Command } from 'commander';

const generators = new Set(['module', 'plugin', 'command', 'conversation']);

export function generateCommand(): Command {
  return new Command('generate')
    .alias('g')
    .description('Генераторы: module, plugin, command, conversation')
    .argument('<type>', 'module | plugin | command | conversation')
    .argument('<name>', 'имя сущности')
    .action(async (type: string, name: string) => {
      if (!generators.has(type)) throw new Error(`Неизвестный генератор: ${type}`);
      const dir = join('generated', type, safeName(name));
      await mkdir(dir, { recursive: true });
      const file = join(dir, 'index.ts');
      await writeFile(file, render(type, name));
      console.log(`Создано: ${file}`);
    });
}

function safeName(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9-_]+/g, '-').replace(/^-+|-+$/g, '');
}

function render(type: string, name: string): string {
  if (type === 'command') return `import type { CommandDefinition } from '@cajeerbots/interaction-core';\n\nexport const command: CommandDefinition = {\n  name: '${safeName(name)}',\n  description: '${name}',\n  async handler(interaction) {\n    console.log('command executed', interaction);\n  },\n};\n`;
  if (type === 'conversation') return `import type { ConversationDefinition } from '@cajeerbots/interaction-core';\n\nexport const conversation: ConversationDefinition = {\n  id: '${safeName(name)}',\n  steps: [{ id: 'start', messageKey: '${safeName(name)}.start' }],\n};\n`;
  const exportName = `generated${type[0]?.toUpperCase() ?? ''}${type.slice(1).replace(/-./g, (part) => part[1]?.toUpperCase() ?? '')}`;
  return `export const ${exportName} = {\n  name: '${safeName(name)}',\n};\n`;
}
