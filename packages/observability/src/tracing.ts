export async function startTracing(): Promise<void> {
  if (process.env.OTEL_ENABLED !== 'true') return;
  const { NodeSDK } = await import('@opentelemetry/sdk-node');
  const sdk = new NodeSDK({ serviceName: process.env.OTEL_SERVICE_NAME ?? 'cajeerbots' });
  await sdk.start();
  process.once('SIGTERM', () => void sdk.shutdown());
  process.once('SIGINT', () => void sdk.shutdown());
}
