import { describe, expect, it } from 'vitest';
import type { Prisma } from '@cajeerbots/db';
import { publishEventSchema } from '../event-api/event-api.schemas.js';
import { enqueueExtensionEventSchema, setExtensionStorageSchema } from '../extensions/extensions.schemas.js';
import { createCallbackActionSchema, createInteractionCommandSchema, createInteractionMenuSchema, updateInteractionMenuSchema } from '../interactions/interactions.schemas.js';
import { incomingEventSchema } from '../webhooks/webhooks.schemas.js';
import { jsonValueSchema } from './json.schemas.js';

describe('API JSON validation and persistence types', () => {
  it('requires event and storage payloads, but accepts explicit JSON null', () => {
    expect(publishEventSchema.safeParse({ type: 'test.event' }).success).toBe(false);
    expect(incomingEventSchema.safeParse({ id: 'e1', type: 'test.event' }).success).toBe(false);
    expect(setExtensionStorageSchema.safeParse({}).success).toBe(false);
    const publish: { payload: unknown } = publishEventSchema.parse({ type: 'test.event', payload: null });
    const webhook: { payload: unknown } = incomingEventSchema.parse({ id: 'e1', type: 'test.event', payload: null });
    const storage: { value: unknown } = setExtensionStorageSchema.parse({ value: null });
    expect(publish.payload).toBeNull();
    expect(webhook.payload).toBeNull();
    expect(storage.value).toBeNull();
  });

  it('preserves nested JSON and falsy values; rejects non-JSON values', () => {
    const value = { items: [0, false, '', null, { nested: ['text'] }] };
    expect(jsonValueSchema.parse(value)).toEqual(value);
    for (const invalid of [undefined, NaN, Infinity, new Date(), () => 1, { x: undefined }, [undefined]]) {
      expect(jsonValueSchema.safeParse(invalid).success).toBe(false);
    }
    for (const item of [0, false, '', [], {}]) expect(setExtensionStorageSchema.parse({ value: item }).value).toEqual(item);
  });

  it('keeps the default extension event payload but does not replace null', () => {
    const event: { hook: string; payload: unknown } = enqueueExtensionEventSchema.parse({ hook: 'test.event' });
    expect(event.payload).toEqual({});
    expect(enqueueExtensionEventSchema.parse({ hook: 'test.event', payload: null }).payload).toBeNull();
  });

  it('produces Prisma-compatible JSON for interaction creates and updates', () => {
    const command: Prisma.InteractionCommandUncheckedCreateInput = {
      communityId: 'c1', ...createInteractionCommandSchema.parse({ name: 'help', description: 'Help', metadata: { nested: [null, false, 0] } }),
    };
    expect(command.metadata).toEqual({ nested: [null, false, 0] });
    const callback: Prisma.InteractionCallbackActionUncheckedCreateInput = {
      communityId: 'c1', ...createCallbackActionSchema.parse({ code: 'help.open', title: 'Help', payload: { target: 'help', value: null } }),
    };
    expect(callback.payload).toEqual({ target: 'help', value: null });
    const menu: Prisma.InteractionMenuUncheckedCreateInput = {
      communityId: 'c1', ...createInteractionMenuSchema.parse({ code: 'home', title: 'Home', buttons: [[{ id: 'a', action: 'help', label: 'Help', payload: { value: false } }]] }),
    };
    expect(menu.buttons).toEqual([[{ id: 'a', action: 'help', label: 'Help', payload: { value: false } }]]);
    const update: Prisma.InteractionMenuUncheckedUpdateInput = updateInteractionMenuSchema.parse({ buttons: [] });
    expect(update.buttons).toEqual([]);
    expect(updateInteractionMenuSchema.parse({ title: 'Changed' })).not.toHaveProperty('buttons');
  });
});
