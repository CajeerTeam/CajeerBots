import { Command } from 'commander';
import { readFile } from 'node:fs/promises';
import { apiClient, print } from '../api-client.js';
export function registryCommand(): Command {
  const command = new Command('registry').description('Registry: signed packages');
  command.command('sources:list').action(async () => print(await apiClient().registrySources()));
  command.command('sources:add').requiredOption('--name <name>').requiredOption('--url <url>').option('--type <type>', 'source type', 'gitflic_registry').option('--public-key <pem>').option('--public-key-file <path>').action(async (o: { name: string; url: string; type: string; publicKey?: string; publicKeyFile?: string }) => print(await apiClient().createRegistrySource({ name: o.name, url: o.url, type: o.type, publicKey: o.publicKeyFile ? await readFile(o.publicKeyFile, 'utf8') : o.publicKey })));
  command.command('sources:sync').argument('<id>').action(async (id: string) => print(await apiClient().syncRegistrySource(id)));
  command.command('packages:list').option('--kind <kind>').option('--source <id>').option('--q <query>').action(async (o: { kind?: string; source?: string; q?: string }) => print(await apiClient().registryPackages({ kind: o.kind, sourceId: o.source, q: o.q })));
  command.command('packages:install').argument('<name>').option('--version <version>').option('--source <id>').option('--auto-grant').action(async (name: string, o: { version?: string; source?: string; autoGrant?: boolean }) => print(await apiClient().installRegistryPackage(name, { version: o.version, sourceId: o.source, autoGrant: o.autoGrant })));
  return command;
}
