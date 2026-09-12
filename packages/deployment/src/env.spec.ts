import { describe, expect, it } from 'vitest';
import { generateProductionEnv, parseEnvText, validateProductionEnv } from './env.js';

describe('production env', () => {
  it('generates valid production env', () => {
    const parsed = parseEnvText(generateProductionEnv({ appUrl: 'https://bots.example.test' }));
    const result = validateProductionEnv(parsed);
    expect(result.valid).toBe(true);
    expect(parsed.APP_PUBLIC_URL).toBe('https://bots.example.test');
  });
});
