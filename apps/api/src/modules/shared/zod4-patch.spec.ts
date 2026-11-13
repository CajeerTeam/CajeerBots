import { describe, expect, it } from 'vitest';
import { createCommunitySchema, updateCommunitySchema } from '../communities/communities.schemas.js';
import {
  createInteractionCommandSchema,
  updateInteractionCommandSchema,
  updateInteractionMenuSchema,
  updateCallbackActionSchema,
  updateConversationFlowSchema,
} from '../interactions/interactions.schemas.js';
import { createRegistrySourceSchema, updateRegistrySourceSchema } from '../registry/registry.schemas.js';

describe('Zod 4 PATCH semantics', () => {
  it('does not apply create defaults to omitted PATCH fields', () => {
    expect(updateCommunitySchema.safeParse({}).success).toBe(false);
    expect(updateCommunitySchema.parse({ title: 'Updated' })).toEqual({ title: 'Updated' });
    expect(updateRegistrySourceSchema.parse({ name: 'Updated' })).toEqual({ name: 'Updated' });
    expect(updateInteractionCommandSchema.parse({ description: 'Updated' })).toEqual({ description: 'Updated' });
    expect(updateInteractionMenuSchema.parse({ title: 'Updated' })).toEqual({ title: 'Updated' });
    expect(updateCallbackActionSchema.parse({ title: 'Updated' })).toEqual({ title: 'Updated' });
    expect(updateConversationFlowSchema.parse({ title: 'Updated' })).toEqual({ title: 'Updated' });
  });

  it('retains create defaults, explicit false, empty collections and null', () => {
    expect(createCommunitySchema.parse({ slug: 'community', title: 'Community' }).language).toBe('ru');
    expect(createRegistrySourceSchema.parse({ name: 'Registry', url: 'https://example.org' }).isActive).toBe(true);
    expect(createInteractionCommandSchema.parse({ name: 'start', description: 'Start' }).type).toBe('command');
    expect(updateInteractionCommandSchema.parse({ isEnabled: false, aliases: [], provider: null })).toEqual({ isEnabled: false, aliases: [], provider: null });
    expect(updateInteractionMenuSchema.parse({ buttons: [], isActive: false })).toEqual({ buttons: [], isActive: false });
  });

  it('continues to reject invalid values and unsafe callback prefixes', () => {
    expect(updateCommunitySchema.safeParse({ title: '' }).success).toBe(false);
    expect(updateInteractionCommandSchema.safeParse({ cooldownSeconds: -1 }).success).toBe(false);
    expect(updateCallbackActionSchema.safeParse({ code: 'rc4:reserved' }).success).toBe(false);
  });
});
