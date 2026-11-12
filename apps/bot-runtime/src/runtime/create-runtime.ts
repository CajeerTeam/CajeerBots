import { PrismaService } from '@cajeerbots/db';
import { SerialPoller } from '@cajeerbots/core';
import { TelegramAdapter } from '../adapters/telegram.adapter.js';
import { VkAdapter } from '../adapters/vk.adapter.js';
import { DiscordAdapter } from '../adapters/discord.adapter.js';
import { InteractionRouter } from '@cajeerbots/interaction-core';
import { loadRuntimeBotAccounts } from './load-bot-accounts.js';
import { configureRouterFromDatabase } from './runtime-interactions.js';
import { startRuntimeStatus, type RuntimeStatusHandle } from './runtime-records.js';
import { BotWebhookServer } from './webhook-server.js';
import type { BotAdapter } from '../adapters/bot-adapter.js';

export interface BotRuntimeOptions { mode: 'polling' | 'webhook'; }

export async function createRuntime(options: BotRuntimeOptions): Promise<{ start(): Promise<void>; stop(): Promise<void> }> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    const router = new InteractionRouter();
    await configureRouterFromDatabase(prisma, router);
    const accounts = await loadRuntimeBotAccounts(prisma, options.mode);
    const adapters: BotAdapter[] = accounts.filter((a) => a.mode === 'polling').map((account) => {
      const adapterOptions = { ...options, account, prisma };
      if (account.provider === 'telegram') return new TelegramAdapter(router, adapterOptions);
      if (account.provider === 'vk') return new VkAdapter(router, adapterOptions);
      return new DiscordAdapter(router, adapterOptions);
    });
    let ready = false;
    let stopped = false;
    const statuses: RuntimeStatusHandle[] = [];
    const started: BotAdapter[] = [];
    const server = new BotWebhookServer(prisma, router, accounts.filter((account) => account.mode === 'webhook'), () => ready, options.mode);
    const heartbeat = new SerialPoller(Number(process.env.BOT_RUNTIME_HEARTBEAT_MS ?? 15_000), async () => {
      await Promise.all(statuses.map((status) => status.beat()));
    }, (error) => console.error('[bot-runtime] heartbeat failed:', error instanceof Error ? error.name : 'unknown'));
    const stop = async () => {
      if (stopped) return;
      stopped = true; ready = false;
      await heartbeat.stop();
      await server.stop();
      await Promise.allSettled(started.map((adapter) => adapter.stop()));
      await Promise.allSettled(statuses.map((status) => status.stop()));
      await prisma.close();
    };
    return {
      async start() {
        try {
          await server.start();
          for (const adapter of adapters) {
            if (stopped) return;
            if (!adapter.isConfigured()) continue;
            started.push(adapter);
            await adapter.start();
          }
          if (stopped) return;
          for (const account of accounts) statuses.push(await startRuntimeStatus(prisma, account));
          await heartbeat.start();
          ready = true;
        } catch (error) { await stop(); throw error; }
      },
      stop,
    };
  } catch (error) { await prisma.close(); throw error; }
}
