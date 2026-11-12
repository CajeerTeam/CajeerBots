import { readFile } from 'node:fs/promises';
import { Command } from 'commander';
import { checkEngineCompatibility, extensionManifestSchema, type ExtensionManifest } from '@cajeerbots/extension-sdk';
import { apiClient, print } from '../api-client.js';

const DEFAULT_ENGINE_API = '1.0.0-rc.4';

export function extensionCommand(): Command {
  const command = new Command('extension').description('Установка, permissions, runtime events и storage расширений');

  command
    .command('validate')
    .argument('<path>', 'путь до manifest расширения')
    .option('--engine-api <version>', 'версия Engine API', DEFAULT_ENGINE_API)
    .description('Проверить manifest, permissions и compatibility')
    .action(async (path: string, options: { engineApi: string }) => {
      const manifest = await readManifest(path);
      const compatibility = checkEngineCompatibility(manifest.engine.range, options.engineApi);
      if (!compatibility.compatible) {
        console.error('Расширение несовместимо:');
        for (const reason of compatibility.reasons) console.error(`- ${reason}`);
        process.exitCode = 1;
        return;
      }
      console.log(JSON.stringify({ ok: true, name: manifest.name, version: manifest.version, hooks: manifest.hooks, permissions: manifest.permissions }, null, 2));
    });

  command.command('list').option('--status <status>').action(async (o: { status?: 'installed' | 'enabled' | 'disabled' | 'failed' }) => print(await apiClient().extensions(o.status)));
  command.command('install').requiredOption('--manifest <path>').option('--auto-grant').description('Register operator-managed local extension (signed packages use registry packages:install)').action(async (o: { manifest: string; autoGrant?: boolean }) => print(await apiClient().installExtension({ manifest: await readManifest(o.manifest), autoGrant: o.autoGrant })));
  command.command('enable').argument('<id>').action(async (id: string) => print(await apiClient().enableExtension(id)));
  command.command('disable').argument('<id>').action(async (id: string) => print(await apiClient().disableExtension(id)));
  command.command('permissions:list').argument('<id>').action(async (id: string) => print(await apiClient().extensionPermissions(id)));
  command.command('permissions:grant').argument('<id>').requiredOption('--permission <scope...>').option('--reason <reason>').action(async (id: string, o: { permission: string[]; reason?: string }) => print(await apiClient().grantExtensionPermissions(id, o.permission, o.reason)));
  command.command('permissions:revoke').argument('<id>').requiredOption('--permission <scope...>').option('--reason <reason>').action(async (id: string, o: { permission: string[]; reason?: string }) => print(await apiClient().revokeExtensionPermissions(id, o.permission, o.reason)));
  command.command('event:emit').argument('<hook>').requiredOption('--extension <id>').option('--payload <json>', 'event payload', '{}').action(async (hook: string, o: { extension: string; payload: string }) => print(await apiClient().enqueueExtensionEvent(o.extension, hook, JSON.parse(o.payload))));
  command.command('events:list').requiredOption('--extension <id>').option('--status <status>').option('--take <n>', 'limit', '50').action(async (o: { extension: string; status?: string; take: string }) => print(await apiClient().extensionEvents(o.extension, o.status, Number(o.take))));
  command.command('storage:set').argument('<id>').requiredOption('--namespace <namespace>').requiredOption('--key <key>').requiredOption('--value <json>').action(async (id: string, o: { namespace: string; key: string; value: string }) => print(await apiClient().setExtensionStorage(id, o.namespace, o.key, JSON.parse(o.value))));
  command.command('storage:list').argument('<id>').option('--namespace <namespace>', 'namespace', 'default').action(async (id: string, o: { namespace: string }) => print(await apiClient().extensionStorage(id, o.namespace)));
  return command;
}
async function readManifest(path: string): Promise<ExtensionManifest> {
  return extensionManifestSchema.parse(JSON.parse(await readFile(path, 'utf8')));
}
