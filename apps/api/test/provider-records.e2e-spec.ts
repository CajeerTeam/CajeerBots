import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { PrismaService, findMessageButtonLayout, upsertInteractionCommand, upsertMessageButtonLayout } from '@cajeerbots/db';

const prisma = new PrismaService();
beforeAll(() => prisma.connect());
afterAll(() => prisma.close());

it('upserts generic provider records in PostgreSQL without duplicates during concurrent writes', async () => {
  const community = await prisma.community.create({ data: { slug: `provider-${randomUUID()}`, title: 'Provider records' } });
  try {
    const command = { communityId: community.id, name: 'help', description: 'Help' };
    const commands = await Promise.all([1, 2].map(() => prisma.$transaction((tx) => upsertInteractionCommand(tx, command, {}))));
    expect(commands[0]?.id).toBe(commands[1]?.id);
    expect(await prisma.interactionCommand.count({ where: { communityId: community.id, name: 'help', provider: null } })).toBe(1);
    const layout = { communityId: community.id, code: 'home', locale: 'ru', rows: [[{ id: 'help', label: 'Help', action: 'help' }]] };
    const layouts = await Promise.all([1, 2].map(() => prisma.$transaction((tx) => upsertMessageButtonLayout(tx, layout, {}))));
    expect(layouts[0]?.id).toBe(layouts[1]?.id);
    expect(await prisma.messageButtonLayout.count({ where: { communityId: community.id, code: 'home', provider: null } })).toBe(1);
    const specific = await prisma.$transaction((tx) => upsertMessageButtonLayout(tx, { ...layout, provider: 'telegram' }, {}));
    expect((await findMessageButtonLayout(prisma, community.id, 'home', 'ru', 'telegram'))?.id).toBe(specific.id);
    expect((await findMessageButtonLayout(prisma, community.id, 'home', 'en', 'discord'))?.id).toBe(layouts[0]?.id);
  } finally {
    await prisma.community.delete({ where: { id: community.id } });
  }
});
