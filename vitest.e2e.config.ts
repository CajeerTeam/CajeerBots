import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['apps/**/*.e2e-spec.ts', 'packages/test-kit/**/*.e2e-spec.ts'],
    testTimeout: 30_000
  }
});
