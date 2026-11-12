import { performance } from 'node:perf_hooks';
export async function runLoad({ count, concurrency, operation, maxP95Ms, minRps = 0 }) {
  if (!Number.isInteger(count) || count < 1 || count > 10000 || !Number.isInteger(concurrency) || concurrency < 1 || concurrency > 64 || !Number.isFinite(maxP95Ms) || maxP95Ms <= 0 || !Number.isFinite(minRps) || minRps < 0) throw new Error('Invalid load bounds');
  let next = 0; const latencies = [], errors = [];
  const started = performance.now();
  await Promise.all(Array.from({ length: Math.min(concurrency, count) }, async () => {
    while (next < count) {
      const index = next++; const at = performance.now();
      try { await operation(index); } catch (error) { errors.push({ index, error: String(error?.message ?? error).slice(0, 250) }); }
      finally { latencies.push(performance.now() - at); }
    }
  }));
  const seconds = (performance.now() - started) / 1000;
  latencies.sort((a, b) => a - b);
  const percentile = (p) => latencies[Math.ceil(p * count) - 1];
  const result = { count, concurrency, succeeded: count - errors.length, failed: errors.length, seconds, rps: (count - errors.length) / seconds, p50Ms: percentile(.5), p95Ms: percentile(.95), p99Ms: percentile(.99), maxMs: latencies.at(-1), errors: errors.slice(0, 10) };
  result.passed = result.failed === 0 && result.p95Ms <= maxP95Ms && result.rps >= minRps;
  return result;
}
