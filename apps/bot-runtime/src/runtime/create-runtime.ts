import { PrismaService } from '@cajeerbots/db';
import { TelegramAdapter } from '../adapters/telegram.adapter.js';
import { VkAdapter } from '../adapters/vk.adapter.js';
import { DiscordAdapter } from '../adapters/discord.adapter.js';
import { InteractionRouter } from '@cajeerbots/interaction-core';
import { loadRuntimeBotAccounts } from './load-bot-accounts.js';
import { configureRouterFromDatabase } from './runtime-interactions.js';
import { startRuntimeStatus, type RuntimeStatusHandle } from './runtime-records.js';
import { BotWebhookServer } from './webhook-server.js';
import type { BotAdapter } from '../adapters/bot-adapter.js';

export interface BotRuntimeOptions {
  mode: 'polling' | 'webhook';
}

export async function createRuntime(options: BotRuntimeOptions): Promise<{ start(): Promise<void>; stop(): Promise<void> }> {
  const prisma = new PrismaService();
  await prisma.connect();

  const router = new InteractionRouter();
  await configureRouterFromDatabase(prisma, router);

  const accounts = await loadRuntimeBotAccounts(prisma, options.mode);
  const adapters: BotAdapter[] = accounts.map((account) => {
    const adapterOptions = { ...options, account, prisma };
    if (account.provider === 'telegram') return new TelegramAdapter(router, adapterOptions);
    if (account.provider === 'vk') return new VkAdapter(router, adapterOptions);
    return new DiscordAdapter(router, adapterOptions);
  });

  let statuses: RuntimeStatusHandle[] = [];
  let heartbeat: NodeJS.Timeout | undefined;
  const webhookServer = options.mode === 'webhook' ? new BotWebhookServer(prisma, router, accounts.filter((account) => account.mode === 'webhook')) : undefined;

  return {
    async start() {
      if (accounts.length === 0) console.warn('[bot-runtime] нет активных bot accounts с валидными tokenRef/env token');
      statuses = await Promise.all(accounts.map((account) => startRuntimeStatus(prisma, account)));
      heartbeat = setInterval(() => void Promise.allSettled(statuses.map((status) => status.beat())), Number(process.env.BOT_RUNTIME_HEARTBEAT_MS ?? 15_000));
      if (webhookServer) await webhookServer.start();
      await Promise.all(adapters.filter((adapter) => adapter.isConfigured()).map((adapter) => adapter.start()));
    },
    async stop() {
      if (heartbeat) clearInterval(heartbeat);
      await Promise.allSettled(adapters.map((adapter) => adapter.stop()));
      await webhookServer?.stop();
      await Promise.allSettled(statuses.map((status) => status.stop()));
      await prisma.close();
    },
  };
}
