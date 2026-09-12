import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { recordDebugEvent, redactDebugMetadata, runBotScenario, persistSimulatedProviderUpdate } from '@cajeerbots/devtools';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class DevtoolsService {
  constructor(private readonly prisma: PrismaService) {}

  async createSession(input: { communityId?: string; title: string; metadata?: Record<string, unknown> }, actor: Principal) {
    if (input.communityId) await this.ensureCommunity(input.communityId);
    const session = await this.prisma.debugSession.create({
      data: {
        communityId: input.communityId ?? null,
        title: input.title,
        actorId: actor.id,
        actorType: actor.type,
        metadata: redactDebugMetadata(input.metadata ?? {}) as never,
      },
    });
    await recordDebugEvent(this.prisma, { sessionId: session.id, communityId: session.communityId, category: 'debug.session', message: `Debug session started: ${session.title}`, metadata: { actorId: actor.id, actorType: actor.type } });
    return session;
  }

  listSessions(query: { communityId?: string; status?: string; take: number }) {
    return this.prisma.debugSession.findMany({
      where: { ...(query.communityId ? { communityId: query.communityId } : {}), ...(query.status ? { status: query.status } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
      include: { _count: { select: { events: true } } },
    });
  }

  async endSession(id: string, actor: Principal) {
    const session = await this.prisma.debugSession.findUnique({ where: { id } });
    if (!session) throw new NotFoundException('Debug session не найдена');
    const updated = await this.prisma.debugSession.update({ where: { id }, data: { status: 'ended', endedAt: new Date() } });
    await recordDebugEvent(this.prisma, { sessionId: id, communityId: session.communityId, category: 'debug.session', message: 'Debug session ended', metadata: { actorId: actor.id, actorType: actor.type } });
    return updated;
  }

  async recordEvent(input: { sessionId?: string; communityId?: string; level: string; category: string; message: string; metadata?: Record<string, unknown> }) {
    if (input.sessionId) {
      const session = await this.prisma.debugSession.findUnique({ where: { id: input.sessionId } });
      if (!session) throw new NotFoundException('Debug session не найдена');
    }
    return recordDebugEvent(this.prisma, { ...input, metadata: redactDebugMetadata(input.metadata ?? {}) as Record<string, unknown> });
  }

  listEvents(query: { sessionId?: string; communityId?: string; level?: string; category?: string; take: number }) {
    return this.prisma.debugEvent.findMany({
      where: {
        ...(query.sessionId ? { sessionId: query.sessionId } : {}),
        ...(query.communityId ? { communityId: query.communityId } : {}),
        ...(query.level ? { level: query.level } : {}),
        ...(query.category ? { category: query.category } : {}),
      },
      orderBy: { createdAt: 'desc' },
      take: query.take,
    });
  }

  async upsertScenario(communityId: string, input: { code: string; title: string; provider?: 'telegram' | 'vk' | 'discord' | null; accountId?: string | null; definition: unknown; isActive: boolean }, actor: Principal) {
    await this.ensureCommunity(communityId);
    const scenario = await this.prisma.botScenario.upsert({
      where: { communityId_code: { communityId, code: input.code } },
      create: { communityId, code: input.code, title: input.title, provider: input.provider as never, accountId: input.accountId ?? null, definition: input.definition as never, isActive: input.isActive },
      update: { title: input.title, provider: input.provider as never, accountId: input.accountId ?? null, definition: input.definition as never, isActive: input.isActive },
    });
    await recordDebugEvent(this.prisma, { communityId, category: 'bot.scenario', message: `Scenario upserted: ${scenario.code}`, metadata: { scenarioId: scenario.id, actorId: actor.id } });
    return scenario;
  }

  listScenarios(communityId: string, query: { provider?: string; active?: boolean; q?: string; take: number }) {
    return this.prisma.botScenario.findMany({
      where: {
        communityId,
        ...(query.provider ? { provider: query.provider as never } : {}),
        ...(query.active === undefined ? {} : { isActive: query.active }),
        ...(query.q ? { OR: [{ code: { contains: query.q, mode: 'insensitive' as const } }, { title: { contains: query.q, mode: 'insensitive' as const } }] } : {}),
      },
      orderBy: [{ isActive: 'desc' }, { code: 'asc' }],
      take: query.take,
      include: { _count: { select: { runs: true } } },
    });
  }

  async getScenario(communityId: string, idOrCode: string) {
    const scenario = await this.prisma.botScenario.findFirst({ where: { communityId, OR: [{ id: idOrCode }, { code: idOrCode }] }, include: { runs: { orderBy: { createdAt: 'desc' }, take: 20 } } });
    if (!scenario) throw new NotFoundException('Bot scenario не найден');
    return scenario;
  }

  async runScenario(communityId: string, idOrCode: string, input: { accountId?: string; provider?: 'telegram' | 'vk' | 'discord'; dryRun: boolean }, actor: Principal) {
    const scenario = await this.prisma.botScenario.findFirst({ where: { communityId, OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!scenario) throw new NotFoundException('Bot scenario не найден');
    if (!scenario.isActive) throw new BadRequestException('Bot scenario отключён');
    const run = await this.prisma.botScenarioRun.create({ data: { scenarioId: scenario.id, communityId, provider: (input.provider ?? scenario.provider) as never, accountId: input.accountId ?? scenario.accountId, triggeredBy: actor.id } });
    const result = await runBotScenario({ prisma: this.prisma, communityId, scenarioId: scenario.id, scenarioCode: scenario.code, definition: scenario.definition, provider: input.provider ?? scenario.provider, accountId: input.accountId ?? scenario.accountId, triggeredBy: actor.id, dryRun: input.dryRun });
    const updated = await this.prisma.botScenarioRun.update({
      where: { id: run.id },
      data: { status: result.status, finishedAt: new Date(), durationMs: result.durationMs, stepsResult: result.steps as never, error: result.error ?? null },
    });
    await recordDebugEvent(this.prisma, { communityId, category: 'bot.scenario.run', level: result.status === 'succeeded' ? 'info' : 'error', message: `Scenario ${scenario.code} ${result.status}`, metadata: { scenarioId: scenario.id, runId: run.id, durationMs: result.durationMs, error: result.error } });
    return updated;
  }

  listRuns(communityId: string, query: { scenarioId?: string; status?: string; take: number }) {
    return this.prisma.botScenarioRun.findMany({
      where: { communityId, ...(query.scenarioId ? { scenarioId: query.scenarioId } : {}), ...(query.status ? { status: query.status } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
      include: { scenario: true },
    });
  }

  async simulateUpdate(communityId: string, input: { accountId: string; provider: 'telegram' | 'vk' | 'discord'; type: string; text?: string; command?: string; callbackCode?: string; userExternalId: string; chatExternalId?: string; payload?: Record<string, unknown> }, actor: Principal) {
    const account = await this.prisma.botAccount.findFirst({ where: { id: input.accountId, communityId, provider: input.provider as never } });
    if (!account) throw new NotFoundException('Bot account не найден для указанного provider/community');
    const inbound = await persistSimulatedProviderUpdate(this.prisma, { ...input, communityId, type: input.type as never, chatExternalId: input.chatExternalId ?? input.userExternalId });
    await recordDebugEvent(this.prisma, { communityId, category: 'bot.update.simulate', message: `Simulated ${input.provider} ${input.type}`, metadata: { inboundUpdateId: inbound.id, accountId: input.accountId, actorId: actor.id } });
    return inbound;
  }

  private async ensureCommunity(communityId: string) {
    const exists = await this.prisma.community.findUnique({ where: { id: communityId }, select: { id: true } });
    if (!exists) throw new NotFoundException('Community не найдена');
  }
}
