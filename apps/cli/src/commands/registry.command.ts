import { readFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { parseRegistryManifest, latestVersion } from '@cajeerbots/registry';
import { checkEngineCompatibility, type ExtensionManifest } from '@cajeerbots/extension-sdk';

const ENGINE_API_VERSION = '1.0.0-rc.2';

export function registryCommand(): Command {
  const command = new Command('registry').description('Extension registry/store, синхронизация manifest и установка packages');

  command.command('sources:list').description('Показать registry sources').action(async () => {
    await withPrisma(async (prisma) => {
      console.log(JSON.stringify(await prisma.registrySource.findMany({ orderBy: { createdAt: 'desc' }, include: { _count: { select: { packages: true } } } }), null, 2));
    });
  });

  command.command('sources:add').requiredOption('--name <name>').requiredOption('--url <url>').option('--type <type>', 'тип source', 'gitflic_registry').option('--public-key <pem>').option('--auth-secret <ref>').description('Добавить registry source').action(async (options: { name: string; url: string; type: string; publicKey?: string; authSecret?: string }) => {
    await withPrisma(async (prisma) => {
      const source = await prisma.registrySource.create({ data: { name: options.name, url: options.url, type: options.type, publicKey: options.publicKey, authSecretRef: options.authSecret } });
      console.log(JSON.stringify(source, null, 2));
    });
  });

  command.command('sources:sync').argument('<idOrName>').description('Синхронизировать source из registry manifest').action(async (idOrName: string) => {
    await withPrisma(async (prisma) => {
      const source = await prisma.registrySource.findFirst({ where: { OR: [{ id: idOrName }, { name: idOrName }] } });
      if (!source) throw new Error('Registry source не найден');
      const manifest = parseRegistryManifest(JSON.parse(await loadText(source.url)));
      let versions = 0;
      for (const item of manifest.packages) {
        const latest = latestVersion(item.versions);
        const pkg = await prisma.registryPackage.upsert({
          where: { sourceId_name: { sourceId: source.id, name: item.name } },
          create: { sourceId: source.id, name: item.name, displayName: item.displayName, description: item.description, kind: item.kind, publisher: item.publisher, homepage: item.homepage, repository: item.repository, license: item.license, isOfficial: item.official, latestVersion: latest?.version, metadata: item.metadata as never },
          update: { displayName: item.displayName, description: item.description, kind: item.kind, publisher: item.publisher, homepage: item.homepage, repository: item.repository, license: item.license, isOfficial: item.official, latestVersion: latest?.version, metadata: item.metadata as never },
        });
        for (const version of item.versions) {
          await prisma.registryPackageVersion.upsert({
            where: { packageId_version: { packageId: pkg.id, version: version.version } },
            create: { packageId: pkg.id, version: version.version, engineRange: version.engine.range, channel: version.channel, manifest: version.manifest as never, artifactUrl: version.artifact.url, artifactSha256: version.artifact.sha256, artifactSize: version.artifact.size, signature: version.artifact.signature, signatureAlgorithm: version.artifact.signatureAlgorithm, releaseNotes: version.releaseNotes, isYanked: version.yanked, metadata: version.metadata as never, publishedAt: version.publishedAt ? new Date(version.publishedAt) : undefined },
            update: { engineRange: version.engine.range, channel: version.channel, manifest: version.manifest as never, artifactUrl: version.artifact.url, artifactSha256: version.artifact.sha256, artifactSize: version.artifact.size, signature: version.artifact.signature, signatureAlgorithm: version.artifact.signatureAlgorithm, releaseNotes: version.releaseNotes, isYanked: version.yanked, metadata: version.metadata as never, publishedAt: version.publishedAt ? new Date(version.publishedAt) : undefined },
          });
          versions += 1;
        }
      }
      await prisma.registrySource.update({ where: { id: source.id }, data: { lastSyncAt: new Date(), syncStatus: 'ok', syncError: null, packageCount: manifest.packages.length } });
      console.log(JSON.stringify({ ok: true, sourceId: source.id, packages: manifest.packages.length, versions }, null, 2));
    });
  });

  command.command('packages:list').option('--kind <kind>').option('--source <id>').option('--q <query>').description('Показать packages из registry').action(async (options: { kind?: string; source?: string; q?: string }) => {
    await withPrisma(async (prisma) => {
      const rows = await prisma.registryPackage.findMany({ where: { ...(options.kind ? { kind: options.kind } : {}), ...(options.source ? { sourceId: options.source } : {}), ...(options.q ? { name: { contains: options.q, mode: 'insensitive' as const } } : {}) }, orderBy: { name: 'asc' }, include: { versions: { orderBy: { createdAt: 'desc' }, take: 5 }, source: true } });
      console.log(JSON.stringify(rows, null, 2));
    });
  });

  command.command('packages:install').argument('<name>').option('--version <version>').option('--auto-grant').description('Установить module/plugin из registry package').action(async (name: string, options: { version?: string; autoGrant?: boolean }) => {
    await withPrisma(async (prisma) => {
      const pkg = await prisma.registryPackage.findFirst({ where: { name }, include: { versions: true } });
      if (!pkg) throw new Error('Registry package не найден');
      const selected = options.version ? pkg.versions.find((version) => version.version === options.version) : pkg.versions.filter((version) => !version.isYanked).sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())[0];
      if (!selected) throw new Error('Версия package не найдена');
      if (!selected.manifest) throw new Error('Версия package не содержит extension manifest');
      const manifest = selected.manifest as ExtensionManifest;
      const compatibility = checkEngineCompatibility(manifest.engine.range, ENGINE_API_VERSION);
      if (!compatibility.compatible) throw new Error(`Расширение несовместимо: ${compatibility.reasons.join('; ')}`);
      const extension = await prisma.extension.create({ data: { name: manifest.name, version: manifest.version, engineRange: manifest.engine.range, manifest: manifest as never, signature: selected.signature ?? manifest.signature, status: 'installed', registrySourceId: pkg.sourceId, registryPackageName: pkg.name, registryVersionId: selected.id } });
      if (options.autoGrant) for (const permission of manifest.permissions) await prisma.extensionPermissionGrant.create({ data: { extensionId: extension.id, permission, actorId: 'cli', reason: 'autoGrant registry install' } });
      console.log(JSON.stringify(extension, null, 2));
    });
  });

  return command;
}

async function loadText(url: string): Promise<string> {
  if (url.startsWith('file://')) return readFile(new URL(url), 'utf8');
  if (url.startsWith('/') || url.startsWith('./') || url.startsWith('../')) return readFile(url, 'utf8');
  const response = await fetch(url, { headers: { accept: 'application/json' } });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.text();
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  try {
    return await callback(prisma);
  } finally {
    await prisma.$disconnect();
  }
}
