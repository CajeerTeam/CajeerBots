import { readFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { checkEngineCompatibility, extensionManifestSchema, type ExtensionManifest } from '@cajeerbots/extension-sdk';
import { DbExtensionRuntime } from '@cajeerbots/extension-runtime';

const DEFAULT_ENGINE_API = '0.6.0';

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

  command.command('list').description('Показать установленные расширения').option('--status <status>', 'installed | enabled | disabled | failed').action(async (options: { status?: string }) => {
    await withPrisma(async (prisma) => {
      const rows = await prisma.extension.findMany({ where: options.status ? { status: options.status as never } : {}, orderBy: { installedAt: 'desc' }, include: { permissionGrants: { where: { revokedAt: null } } } });
      console.log(JSON.stringify(rows, null, 2));
    });
  });

  command.command('install')
    .description('Установить расширение из cajeer.extension.json в БД')
    .requiredOption('--manifest <path>', 'путь к cajeer.extension.json')
    .option('--signature <signature>', 'подпись artifact/manifest')
    .option('--auto-grant', 'автоматически выдать permissions из manifest')
    .action(async (options: { manifest: string; signature?: string; autoGrant?: boolean }) => {
      const manifest = await readManifest(options.manifest);
      const compatibility = checkEngineCompatibility(manifest.engine.range, DEFAULT_ENGINE_API);
      if (!compatibility.compatible) throw new Error(`Расширение несовместимо: ${compatibility.reasons.join('; ')}`);
      await withPrisma(async (prisma) => {
        const extension = await prisma.extension.create({ data: { name: manifest.name, version: manifest.version, engineRange: manifest.engine.range, manifest: manifest as never, signature: options.signature ?? manifest.signature, status: 'installed' } });
        if (options.autoGrant) {
          for (const permission of manifest.permissions) await prisma.extensionPermissionGrant.create({ data: { extensionId: extension.id, permission, actorId: 'cli', reason: 'auto-grant через CLI' } });
        }
        await prisma.auditLog.create({ data: { actorType: 'system', actorId: 'cli', action: 'extension.install', resource: 'extension', resourceId: extension.id, metadata: { name: manifest.name, version: manifest.version, autoGrant: Boolean(options.autoGrant) } as never } });
        console.log(JSON.stringify(extension, null, 2));
      });
    });

  command.command('enable').argument('<id>').description('Включить расширение').action(async (id: string) => {
    await withPrisma(async (prisma) => {
      const extension = await prisma.extension.update({ where: { id }, data: { status: 'enabled' } });
      await new DbExtensionRuntime(prisma).enqueueHook({ extensionId: id, hook: 'extension.enabled', payload: { id, name: extension.name, version: extension.version } });
      console.log(JSON.stringify(extension, null, 2));
    });
  });

  command.command('disable').argument('<id>').description('Отключить расширение').action(async (id: string) => {
    await withPrisma(async (prisma) => {
      const extension = await prisma.extension.update({ where: { id }, data: { status: 'disabled' } });
      console.log(JSON.stringify(extension, null, 2));
    });
  });

  command.command('permissions:list').argument('<id>').description('Показать permission grants расширения').action(async (id: string) => {
    await withPrisma(async (prisma) => {
      const grants = await prisma.extensionPermissionGrant.findMany({ where: { extensionId: id }, orderBy: { grantedAt: 'desc' } });
      console.log(JSON.stringify(grants, null, 2));
    });
  });

  command.command('permissions:grant').argument('<id>').requiredOption('--permission <scope...>', 'permission scopes').option('--reason <reason>', 'причина').description('Выдать permissions расширению').action(async (id: string, options: { permission: string[]; reason?: string }) => {
    await withPrisma(async (prisma) => {
      for (const permission of options.permission) {
        await prisma.extensionPermissionGrant.upsert({ where: { extensionId_permission: { extensionId: id, permission } }, create: { extensionId: id, permission, actorId: 'cli', reason: options.reason }, update: { revokedAt: null, actorId: 'cli', reason: options.reason } });
      }
      console.log(JSON.stringify({ extensionId: id, granted: options.permission }, null, 2));
    });
  });

  command.command('permissions:revoke').argument('<id>').requiredOption('--permission <scope...>', 'permission scopes').option('--reason <reason>', 'причина').description('Отозвать permissions расширения').action(async (id: string, options: { permission: string[]; reason?: string }) => {
    await withPrisma(async (prisma) => {
      await prisma.extensionPermissionGrant.updateMany({ where: { extensionId: id, permission: { in: options.permission } }, data: { revokedAt: new Date(), actorId: 'cli', reason: options.reason } });
      console.log(JSON.stringify({ extensionId: id, revoked: options.permission }, null, 2));
    });
  });

  command.command('event:emit').argument('<hook>').option('--extension <id>', 'target extension id').option('--payload <json>', 'payload JSON', '{}').description('Поставить runtime event расширения в очередь').action(async (hook: string, options: { extension?: string; payload: string }) => {
    await withPrisma(async (prisma) => {
      const result = await new DbExtensionRuntime(prisma).enqueueHook({ extensionId: options.extension, hook, payload: JSON.parse(options.payload) });
      console.log(JSON.stringify(result, null, 2));
    });
  });

  command.command('events:list').option('--extension <id>', 'extension id').option('--status <status>', 'pending | failed | handled | dead').option('--take <n>', 'limit', '50').description('Показать runtime events').action(async (options: { extension?: string; status?: string; take: string }) => {
    await withPrisma(async (prisma) => {
      const rows = await prisma.extensionRuntimeEvent.findMany({ where: { ...(options.extension ? { extensionId: options.extension } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 200) });
      console.log(JSON.stringify(rows, null, 2));
    });
  });

  command.command('storage:set').argument('<id>').requiredOption('--namespace <namespace>', 'namespace', 'default').requiredOption('--key <key>', 'key').requiredOption('--value <json>', 'JSON value').description('Записать значение в storage расширения').action(async (id: string, options: { namespace: string; key: string; value: string }) => {
    await withPrisma(async (prisma) => {
      const value = JSON.parse(options.value);
      const row = await prisma.extensionStorageItem.upsert({ where: { extensionId_namespace_key: { extensionId: id, namespace: options.namespace, key: options.key } }, create: { extensionId: id, namespace: options.namespace, key: options.key, value: value as never }, update: { value: value as never } });
      console.log(JSON.stringify(row, null, 2));
    });
  });

  command.command('storage:list').argument('<id>').option('--namespace <namespace>', 'namespace', 'default').description('Показать storage расширения').action(async (id: string, options: { namespace: string }) => {
    await withPrisma(async (prisma) => {
      const rows = await prisma.extensionStorageItem.findMany({ where: { extensionId: id, namespace: options.namespace }, orderBy: { key: 'asc' } });
      console.log(JSON.stringify(rows, null, 2));
    });
  });

  return command;
}

async function readManifest(path: string): Promise<ExtensionManifest> {
  const raw = await readFile(path, 'utf8');
  return extensionManifestSchema.parse(JSON.parse(raw));
}

async function withPrisma<T>(fn: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    return await fn(prisma);
  } finally {
    await prisma.close();
  }
}
