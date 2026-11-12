import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { buildReleaseManifest, checkDockerComposeProduction, fileToReleaseInput, generateProductionEnv, parseEnvText, renderDefaultSystemdUnits, renderNginxConfig, validateProductionEnv } from '@cajeerbots/deployment';

export function deploymentCommand(): Command {
  const command = new Command('deployment').description('Production deployment, release manifest и конфигурации окружения');

  command.command('targets:list')
    .option('--environment <environment>')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { environment?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.deploymentTarget.findMany({ where: { ...(options.environment ? { environment: options.environment } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: [{ environment: 'asc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('targets:set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .option('--kind <kind>', 'docker_compose | systemd | custom', 'docker_compose')
    .option('--environment <environment>', 'environment', 'production')
    .option('--base-url <url>')
    .option('--compose-file <file...>')
    .option('--env-file <file>')
    .action(async (options: { code: string; title: string; kind: string; environment: string; baseUrl?: string; composeFile?: string[]; envFile?: string }) => withPrisma(async (prisma) => {
      const target = await prisma.deploymentTarget.upsert({
        where: { code: options.code },
        create: { code: options.code, title: options.title, kind: options.kind, environment: options.environment, baseUrl: options.baseUrl, composeFiles: options.composeFile ?? ['docker-compose.prod.yml'], envFile: options.envFile },
        update: { title: options.title, kind: options.kind, environment: options.environment, baseUrl: options.baseUrl, composeFiles: options.composeFile ?? ['docker-compose.prod.yml'], envFile: options.envFile },
      });
      console.log(JSON.stringify(target, null, 2));
    }));

  command.command('checks:run')
    .option('--target <targetId>')
    .option('--root <rootDir>', 'project root', process.cwd())
    .option('--file <file...>', 'compose/env files to check')
    .action(async (options: { target?: string; root: string; file?: string[] }) => withPrisma(async (prisma) => {
      const checks = await checkDockerComposeProduction(options.root, options.file ?? ['docker-compose.prod.yml', '.env.production']);
      const created = [];
      for (const check of checks) {
        created.push(await prisma.deploymentCheck.create({ data: { targetId: options.target ?? null, code: check.code, status: check.status, severity: check.severity, message: check.message, details: { ...(check.details ?? {}), rootDir: options.root } as never } }));
      }
      if (options.target) {
        const status = checks.some((check) => check.status === 'fail') ? 'failed' : checks.some((check) => check.status === 'warn') ? 'warning' : 'ready';
        await prisma.deploymentTarget.update({ where: { id: options.target }, data: { status, lastCheckedAt: new Date() } });
      }
      console.log(JSON.stringify({ checks: created }, null, 2));
    }));

  command.command('checks:list')
    .option('--target <targetId>')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { target?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.deploymentCheck.findMany({ where: { ...(options.target ? { targetId: options.target } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { checkedAt: 'desc' }, take: Math.min(Number(options.take), 1000) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('env:generate')
    .option('--app-url <url>', 'public HTTPS URL')
    .option('--database-url <url>')
    .option('--redis-url <url>')
    .option('--bot-webhook-public-url <url>')
    .option('--storage-driver <driver>', 'local | s3')
    .option('--out <file>', 'write to file')
    .action(async (options: { appUrl?: string; databaseUrl?: string; redisUrl?: string; botWebhookPublicUrl?: string; storageDriver?: 'local' | 's3'; out?: string }) => {
      const content = generateProductionEnv(options);
      await writeOutput(options.out, content);
    });

  command.command('env:validate')
    .argument('<file>')
    .action(async (file: string) => {
      const result = validateProductionEnv(parseEnvText(await readFile(file, 'utf8')));
      console.log(JSON.stringify(result, null, 2));
      if (!result.valid) process.exitCode = 1;
    });

  command.command('nginx:config')
    .requiredOption('--server-name <name>')
    .option('--api-upstream <url>', 'API upstream', 'http://127.0.0.1:3000')
    .option('--bot-upstream <url>', 'Bot webhook upstream', 'http://127.0.0.1:3101')
    .option('--tls')
    .option('--out <file>')
    .action(async (options: { serverName: string; apiUpstream: string; botUpstream: string; tls?: boolean; out?: string }) => {
      const content = renderNginxConfig({ serverName: options.serverName, apiUpstream: options.apiUpstream, botWebhookUpstream: options.botUpstream, enableTls: Boolean(options.tls) });
      await writeOutput(options.out, content);
    });

  command.command('systemd:units')
    .option('--workdir <dir>', 'project workdir', '/opt/cajeerbots')
    .option('--env-file <file>', 'environment file', '/etc/cajeerbots/cajeerbots.env')
    .option('--out-dir <dir>')
    .action(async (options: { workdir: string; envFile: string; outDir?: string }) => {
      const units = renderDefaultSystemdUnits(options.workdir, options.envFile);
      if (options.outDir) {
        await mkdir(options.outDir, { recursive: true });
        for (const [name, content] of Object.entries(units)) await writeFile(join(options.outDir, name), content);
        console.log(JSON.stringify({ written: Object.keys(units).map((name) => join(options.outDir!, name)) }, null, 2));
      } else {
        console.log(JSON.stringify(units, null, 2));
      }
    });

  command.command('release:manifest')
    .requiredOption('--version <version>')
    .option('--channel <channel>', 'alpha | beta | rc | stable | lts | security', 'alpha')
    .option('--file <path...>')
    .option('--out <file>')
    .action(async (options: { version: string; channel: 'alpha' | 'beta' | 'rc' | 'stable' | 'lts' | 'security'; file?: string[]; out?: string }) => {
      const files = [];
      for (const file of options.file ?? []) files.push(await fileToReleaseInput(file, basename(file)));
      const manifest = buildReleaseManifest({ product: 'cajeerbots', version: options.version, channel: options.channel, files });
      const content = JSON.stringify(manifest, null, 2) + '\n';
      await writeOutput(options.out, content);
    });

  return command;
}

async function writeOutput(file: string | undefined, content: string): Promise<void> {
  if (!file) {
    process.stdout.write(content);
    return;
  }
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, content);
  console.log(JSON.stringify({ written: file }, null, 2));
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
