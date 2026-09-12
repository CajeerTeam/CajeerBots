import { OutboxWorker } from './workers/outbox.worker.js';
import { ExtensionEventWorker } from './workers/extensions.worker.js';
import { BotOutboundWorker } from './workers/bot-outbound.worker.js';
import { EventApiDeliveryWorker } from './workers/event-api.worker.js';
import { SchedulerWorker } from './workers/scheduler.worker.js';
import { BackupRetentionWorker } from './workers/backup-retention.worker.js';
import { ObservabilityWorker } from './workers/observability.worker.js';
import { IntegrationTestsWorker } from './workers/integration-tests.worker.js';

const outboxWorker = new OutboxWorker();
const extensionWorker = new ExtensionEventWorker();
const botOutboundWorker = new BotOutboundWorker();
const eventApiDeliveryWorker = new EventApiDeliveryWorker();
const schedulerWorker = new SchedulerWorker();
const backupRetentionWorker = new BackupRetentionWorker();
const observabilityWorker = new ObservabilityWorker();
const integrationTestsWorker = new IntegrationTestsWorker();

async function stop(): Promise<void> {
  await Promise.allSettled([outboxWorker.stop(), extensionWorker.stop(), botOutboundWorker.stop(), eventApiDeliveryWorker.stop(), schedulerWorker.stop(), backupRetentionWorker.stop(), observabilityWorker.stop(), integrationTestsWorker.stop()]);
}

process.once('SIGTERM', () => void stop().then(() => process.exit(0)));
process.once('SIGINT', () => void stop().then(() => process.exit(0)));

await Promise.all([outboxWorker.start(), extensionWorker.start(), botOutboundWorker.start(), eventApiDeliveryWorker.start(), schedulerWorker.start(), backupRetentionWorker.start(), observabilityWorker.start(), integrationTestsWorker.start()]);
