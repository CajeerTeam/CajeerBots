export interface CajeerClientOptions {
  baseUrl: string;
  apiKey?: string;
  jwt?: string;
}

export interface HealthReport {
  status: 'ok' | 'degraded' | 'down';
  checks: Record<string, { status: string; message: string; latencyMs?: number }>;
}

export interface Community {
  id: string;
  slug: string;
  title: string;
  language: string;
  createdAt: string;
  updatedAt: string;
}

export interface ApiKeyRecord {
  id: string;
  title: string;
  scopes: string[];
  expiresAt?: string | null;
  lastUsedAt?: string | null;
  createdAt: string;
  revokedAt?: string | null;
}

export interface IssuedApiKey extends ApiKeyRecord {
  token: string;
}

export interface AdminAccountRecord {
  id: string;
  email: string;
  displayName?: string | null;
  scopes: string[];
  isActive: boolean;
  twoFactorEnabled: boolean;
  twoFactorConfirmedAt?: string | null;
  lastLoginAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface AdminSessionRecord {
  id: string;
  accountId: string;
  ip?: string | null;
  userAgent?: string | null;
  expiresAt: string;
  lastUsedAt?: string | null;
  revokedAt?: string | null;
  createdAt: string;
}

export interface LoginResult {
  account: AdminAccountRecord;
  accessToken: string;
  accessTokenExpiresAt: string;
  refreshToken: string;
  refreshTokenExpiresAt: string;
  session: AdminSessionRecord;
}

export interface UserRecord {
  id: string;
  communityId: string;
  provider: 'telegram' | 'vk' | 'discord';
  externalId: string;
  username?: string | null;
  displayName?: string | null;
  locale: string;
}

export interface RoleRecord {
  id: string;
  communityId: string;
  code: string;
  title: string;
  priority: number;
}

export interface BotAccountRecord {
  id: string;
  communityId: string;
  provider: 'telegram' | 'vk' | 'discord';
  mode: 'polling' | 'webhook';
  externalId?: string | null;
  username?: string | null;
  tokenRef?: string | null;
  isActive: boolean;
}



export interface LocaleCatalogRecord {
  id: string;
  communityId: string;
  namespace: string;
  locale: string;
  fallbackLocale?: string | null;
  messages: Record<string, string>;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export interface MessageButtonLayoutRecord {
  id: string;
  communityId: string;
  code: string;
  provider?: 'telegram' | 'vk' | 'discord' | null;
  locale: string;
  rows: unknown;
  createdAt: string;
  updatedAt: string;
}

export interface MessageFormDefinitionRecord {
  id: string;
  communityId: string;
  code: string;
  title: string;
  locale: string;
  fields: unknown;
  submitTemplateKey?: string | null;
  successTemplateKey?: string | null;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface MessageFormSubmissionRecord {
  id: string;
  communityId: string;
  formId: string;
  provider?: 'telegram' | 'vk' | 'discord' | null;
  userExternalId?: string | null;
  payload: unknown;
  normalized?: unknown;
  errors?: unknown;
  status: string;
  createdAt: string;
}

export interface MessageRenderResult {
  text: string;
  locale: string;
  provider?: 'telegram' | 'vk' | 'discord';
  variablesUsed: string[];
  buttons?: unknown;
  rawButtons?: unknown;
  renderLogId: string;
}

export interface MessageTemplateRecord {
  id: string;
  key: string;
  locale: string;
  body: string;
  variables?: unknown;
  buttons?: unknown;
}


export interface SecretRecord {
  id: string;
  key: string;
  ref: string;
  description?: string | null;
  version: number;
  createdAt: string;
  updatedAt: string;
  rotatedAt?: string | null;
}

export interface RuntimeConfigSummary {
  app: Record<string, unknown>;
  runtime: Record<string, unknown>;
  configured: Record<string, boolean>;
}

export interface CommunityExportPayload {
  kind: 'cajeerbots.community.export';
  version: string;
  exportedAt: string;
  community: { slug: string; title: string; language: string };
  settings: Array<{ key: string; value: unknown }>;
  roles: Array<{ code: string; title: string; priority: number; permissions: string[] }>;
  users: Array<{ externalId: string; provider: 'telegram' | 'vk' | 'discord'; username?: string | null; displayName?: string | null; locale: string; roles: string[] }>;
  botAccounts: Array<{ provider: 'telegram' | 'vk' | 'discord'; mode: 'polling' | 'webhook'; externalId?: string | null; username?: string | null; tokenRef?: string | null; isActive: boolean }>;
  messageTemplates: Array<{ key: string; locale: string; body: string; variables?: unknown; buttons?: unknown }>;
  localeCatalogs?: Array<{ namespace: string; locale: string; fallbackLocale?: string | null; messages: unknown }>;
  buttonLayouts?: Array<{ code: string; provider?: 'telegram' | 'vk' | 'discord' | null; locale: string; rows: unknown }>;
  messageForms?: Array<{ code: string; title: string; locale: string; fields: unknown; submitTemplateKey?: string | null; successTemplateKey?: string | null; isActive: boolean }>;
  eventTopics?: Array<{ name: string; description?: string | null; schema?: unknown; isActive: boolean; retentionDays: number }>;
  eventSubscriptions?: Array<{ name: string; topicPattern: string; targetType: string; targetRef: string; filters?: unknown; isActive: boolean; maxAttempts: number }>;
}

export interface InteractionCommandRecord {
  id: string;
  communityId: string;
  name: string;
  description: string;
  provider?: 'telegram' | 'vk' | 'discord' | null;
  type: string;
  aliases: string[];
  scopes: string[];
  handler: string;
  templateKey?: string | null;
  conversationCode?: string | null;
  cooldownSeconds: number;
  isEnabled: boolean;
  metadata?: unknown;
}

export interface InteractionMenuRecord {
  id: string;
  communityId: string;
  code: string;
  title: string;
  locale: string;
  templateKey?: string | null;
  buttons: unknown;
  isActive: boolean;
}

export interface CallbackActionRecord {
  id: string;
  communityId: string;
  code: string;
  title: string;
  handler: string;
  scopes: string[];
  payload?: unknown;
  isActive: boolean;
}

export interface ConversationFlowRecord {
  id: string;
  communityId: string;
  code: string;
  title: string;
  entryCommand?: string | null;
  steps: unknown;
  isActive: boolean;
}

export interface InteractionEventRecord {
  id: string;
  communityId: string;
  accountId?: string | null;
  provider: 'telegram' | 'vk' | 'discord';
  type: string;
  name?: string | null;
  userExternalId: string;
  status: string;
  payload?: unknown;
  response?: unknown;
  error?: string | null;
  createdAt: string;
}


export interface ExtensionRecord {
  id: string;
  name: string;
  version: string;
  engineRange: string;
  status: 'installed' | 'enabled' | 'disabled' | 'failed';
  manifest: unknown;
  signature?: string | null;
  installedAt: string;
  updatedAt: string;
}

export interface ExtensionPermissionGrantRecord {
  id: string;
  extensionId: string;
  permission: string;
  grantedAt: string;
  revokedAt?: string | null;
  actorId?: string | null;
  reason?: string | null;
}

export interface ExtensionRuntimeEventRecord {
  id: string;
  extensionId?: string | null;
  hook: string;
  status: string;
  payload: unknown;
  result?: unknown;
  error?: string | null;
  attempts: number;
  availableAt: string;
  createdAt: string;
}

export interface ExtensionStorageItemRecord {
  id: string;
  extensionId: string;
  namespace: string;
  key: string;
  value: unknown;
  createdAt: string;
  updatedAt: string;
}


export interface BotRuntimeStatusRecord {
  accountId: string;
  provider: 'telegram' | 'vk' | 'discord';
  mode: 'polling' | 'webhook';
  username?: string | null;
  isActive: boolean;
  runtime: null | {
    id: string;
    status: string;
    version: string;
    hostname?: string | null;
    pid?: number | null;
    startedAt: string;
    lastBeatAt: string;
    stoppedAt?: string | null;
    error?: string | null;
    stale: boolean;
  };
}

export interface BotInboundUpdateRecord {
  id: string;
  accountId: string;
  communityId: string;
  provider: 'telegram' | 'vk' | 'discord';
  providerUpdateId?: string | null;
  type: string;
  userExternalId?: string | null;
  chatExternalId?: string | null;
  payload: unknown;
  status: string;
  receivedAt: string;
  handledAt?: string | null;
  error?: string | null;
}

export interface BotOutboundMessageRecord {
  id: string;
  accountId: string;
  communityId: string;
  inboundUpdateId?: string | null;
  provider: 'telegram' | 'vk' | 'discord';
  targetType: string;
  targetId: string;
  text: string;
  buttons?: unknown;
  status: 'pending' | 'sending' | 'sent' | 'failed' | 'dead';
  attempts: number;
  providerMessageId?: string | null;
  lastError?: string | null;
  availableAt: string;
  sentAt?: string | null;
  createdAt: string;
  updatedAt: string;
}


export interface EventTopicRecord {
  id: string;
  name: string;
  description?: string | null;
  schema?: unknown;
  isActive: boolean;
  retentionDays: number;
  createdAt: string;
  updatedAt: string;
}

export interface EventRecordRecord {
  id: string;
  eventId: string;
  topicName: string;
  type: string;
  source: string;
  communityId?: string | null;
  aggregateType?: string | null;
  aggregateId?: string | null;
  payload: unknown;
  metadata?: unknown;
  headers?: unknown;
  outboxMessageId?: string | null;
  replayCount: number;
  replayedAt?: string | null;
  publishedAt: string;
}

export interface EventSubscriptionRecord {
  id: string;
  name: string;
  topicPattern: string;
  targetType: 'outbox' | 'webhook_endpoint' | 'extension_hook' | 'log' | string;
  targetRef: string;
  filters?: unknown;
  isActive: boolean;
  maxAttempts: number;
  lastDeliveredAt?: string | null;
  disabledAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface EventSubscriptionDeliveryRecord {
  id: string;
  subscriptionId: string;
  eventRecordId: string;
  status: 'pending' | 'processing' | 'delivered' | 'failed' | 'dead';
  attempts: number;
  availableAt: string;
  lockedAt?: string | null;
  responseStatus?: number | null;
  responseBody?: string | null;
  result?: unknown;
  lastError?: string | null;
  deliveredAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface EventDeadLetterRecord {
  id: string;
  sourceType: string;
  sourceId: string;
  reason: string;
  payload: unknown;
  metadata?: unknown;
  status: string;
  createdAt: string;
  resolvedAt?: string | null;
  resolvedBy?: string | null;
}


export interface RegistrySourceRecord {
  id: string;
  name: string;
  url: string;
  type: string;
  authSecretRef?: string | null;
  publicKey?: string | null;
  isActive: boolean;
  lastSyncAt?: string | null;
  syncStatus: string;
  syncError?: string | null;
  packageCount: number;
  createdAt: string;
  updatedAt: string;
}

export interface RegistryPackageRecord {
  id: string;
  sourceId: string;
  name: string;
  displayName?: string | null;
  description?: string | null;
  kind: string;
  publisher?: string | null;
  homepage?: string | null;
  repository?: string | null;
  license?: string | null;
  isOfficial: boolean;
  latestVersion?: string | null;
  metadata?: unknown;
  versions?: RegistryPackageVersionRecord[];
}

export interface RegistryPackageVersionRecord {
  id: string;
  packageId: string;
  version: string;
  engineRange: string;
  channel: string;
  manifest?: unknown;
  artifactUrl: string;
  artifactSha256: string;
  artifactSize?: number | null;
  signature?: string | null;
  signatureAlgorithm: string;
  releaseNotes?: string | null;
  isYanked: boolean;
  publishedAt?: string | null;
  createdAt: string;
}

export interface UpdatePlanRecord {
  id: string;
  targetType: string;
  targetRef?: string | null;
  packageName: string;
  currentVersion: string;
  targetVersion: string;
  sourceId?: string | null;
  versionId?: string | null;
  status: string;
  steps: unknown;
  metadata?: unknown;
  createdAt: string;
  updatedAt: string;
  appliedAt?: string | null;
  error?: string | null;
}

export interface UpdateHistoryRecord {
  id: string;
  planId?: string | null;
  targetType: string;
  targetRef?: string | null;
  packageName: string;
  fromVersion: string;
  toVersion: string;
  status: string;
  steps?: unknown;
  metadata?: unknown;
  error?: string | null;
  createdAt: string;
  completedAt?: string | null;
}

export interface ScheduledJobRecord {
  id: string;
  code: string;
  title: string;
  type: string;
  handler?: string | null;
  schedule: string;
  timezone: string;
  payload?: unknown;
  isEnabled: boolean;
  maxAttempts: number;
  timeoutSeconds: number;
  lockUntil?: string | null;
  lastRunAt?: string | null;
  nextRunAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ScheduledJobRunRecord {
  id: string;
  jobId: string;
  status: string;
  attempt: number;
  triggeredBy: string;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  result?: unknown;
  error?: string | null;
  logs?: unknown;
  createdAt: string;
  job?: ScheduledJobRecord;
}

export interface MaintenanceSnapshotRecord {
  id: string;
  kind: string;
  status: string;
  metrics: unknown;
  startedAt: string;
  finishedAt?: string | null;
  createdAt: string;
}


export interface DebugSessionRecord {
  id: string;
  communityId?: string | null;
  title: string;
  status: string;
  actorId?: string | null;
  actorType?: string | null;
  metadata?: unknown;
  createdAt: string;
  updatedAt: string;
  endedAt?: string | null;
}

export interface DebugEventRecord {
  id: string;
  sessionId?: string | null;
  communityId?: string | null;
  level: string;
  category: string;
  message: string;
  metadata?: unknown;
  createdAt: string;
}

export interface BotScenarioRecord {
  id: string;
  communityId: string;
  code: string;
  title: string;
  provider?: 'telegram' | 'vk' | 'discord' | null;
  accountId?: string | null;
  definition: unknown;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface BotScenarioRunRecord {
  id: string;
  scenarioId: string;
  communityId: string;
  status: string;
  provider?: 'telegram' | 'vk' | 'discord' | null;
  accountId?: string | null;
  triggeredBy: string;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  stepsResult?: unknown;
  error?: string | null;
  createdAt: string;
  scenario?: BotScenarioRecord;
}


export interface StorageObjectRecord {
  id: string;
  key: string;
  driver: string;
  bucket?: string | null;
  contentType: string;
  sizeBytes: number;
  sha256: string;
  metadata?: unknown;
  refType?: string | null;
  refId?: string | null;
  status: string;
  createdAt: string;
  updatedAt: string;
  deletedAt?: string | null;
}

export interface BackupArtifactRecord {
  id: string;
  scope: string;
  communityId?: string | null;
  title: string;
  storageObjectId?: string | null;
  storageKey: string;
  format: string;
  sizeBytes: number;
  sha256: string;
  status: string;
  includedResources: string[];
  encryptionRef?: string | null;
  metadata?: unknown;
  createdAt: string;
  completedAt?: string | null;
  error?: string | null;
}

export interface BackupRestoreRunRecord {
  id: string;
  artifactId: string;
  communityId?: string | null;
  mode: string;
  strategy: string;
  status: string;
  actorId?: string | null;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  result?: unknown;
  error?: string | null;
  createdAt: string;
}


export interface ObservabilityMetricSampleRecord {
  id: string;
  communityId?: string | null;
  source: string;
  metric: string;
  value: number;
  unit?: string | null;
  labels?: unknown;
  severity: string;
  collectedAt: string;
}

export interface ObservabilityProbeRecord {
  id: string;
  code: string;
  title: string;
  type: string;
  target?: string | null;
  intervalSeconds: number;
  timeoutMs: number;
  isEnabled: boolean;
  status: string;
  latencyMs?: number | null;
  lastCheckedAt?: string | null;
  nextCheckAt?: string | null;
  lastError?: string | null;
}

export interface ObservabilityAlertRuleRecord {
  id: string;
  code: string;
  title: string;
  metric: string;
  operator: string;
  threshold: number;
  severity: string;
  isEnabled: boolean;
  lastTriggeredAt?: string | null;
}

export interface ObservabilityIncidentRecord {
  id: string;
  code: string;
  title: string;
  status: string;
  severity: string;
  source: string;
  metric?: string | null;
  currentValue?: number | null;
  startedAt: string;
  resolvedAt?: string | null;
}

export interface DeploymentTargetRecord {
  id: string;
  code: string;
  title: string;
  kind: string;
  environment: string;
  baseUrl?: string | null;
  composeFiles: string[];
  envFile?: string | null;
  status: string;
  lastCheckedAt?: string | null;
  metadata?: unknown;
  createdAt: string;
  updatedAt: string;
}

export interface DeploymentCheckRecord {
  id: string;
  targetId?: string | null;
  code: string;
  status: string;
  severity: string;
  message: string;
  details?: unknown;
  checkedAt: string;
}

export interface DeploymentRunRecord {
  id: string;
  targetId?: string | null;
  action: string;
  status: string;
  actorId?: string | null;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  logs?: unknown;
  metadata?: unknown;
  error?: string | null;
  createdAt: string;
}

export interface ReleaseArtifactRecord {
  id: string;
  version: string;
  channel: string;
  artifactType: string;
  filename: string;
  storageObjectId?: string | null;
  sha256: string;
  sizeBytes: number;
  manifest?: unknown;
  status: string;
  signature?: string | null;
  signedAt?: string | null;
  createdAt: string;
}


export interface ApiContractSnapshotRecord {
  id: string;
  version: string;
  sourcePath: string;
  sourceSha256: string;
  operationCount: number;
  pathCount: number;
  tagCount: number;
  duplicateOperationIds: string[];
  unsecuredOperations?: unknown;
  deprecatedOperations?: unknown;
  snapshot: unknown;
  isBaseline: boolean;
  createdBy?: string | null;
  createdAt: string;
}

export interface ApiContractCheckRecord {
  id: string;
  snapshotId?: string | null;
  baselineId?: string | null;
  status: string;
  checkType: string;
  summary?: unknown;
  issues?: unknown;
  breakingCount: number;
  coveragePercent?: number | null;
  createdBy?: string | null;
  createdAt: string;
}

export interface SdkGenerationRunRecord {
  id: string;
  snapshotId?: string | null;
  status: string;
  generatorVersion: string;
  outputMode: string;
  artifactName?: string | null;
  artifactSha256?: string | null;
  artifactSizeBytes?: number | null;
  operationCount: number;
  coveragePercent?: number | null;
  missingOperations?: unknown;
  generatedFiles?: unknown;
  error?: string | null;
  createdBy?: string | null;
  createdAt: string;
}

export interface ApiDeprecationRecord {
  id: string;
  operationId: string;
  method: string;
  path: string;
  status: string;
  reason?: string | null;
  replacement?: string | null;
  deprecatedSince?: string | null;
  removeAfter?: string | null;
  metadata?: unknown;
  createdBy?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface DocumentationCollectionRecord {
  id: string;
  code: string;
  title: string;
  locale: string;
  sourceType: string;
  sourcePath: string;
  sourceUrl?: string | null;
  isActive: boolean;
  version: number;
  metadata?: unknown;
  lastSyncedAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface DocumentationPageRecord {
  id: string;
  collectionId: string;
  slug: string;
  path: string;
  title: string;
  locale: string;
  status: string;
  body: string;
  excerpt?: string | null;
  headings?: unknown;
  links?: unknown;
  frontmatter?: unknown;
  tags: string[];
  wordCount: number;
  sizeBytes: number;
  sha256: string;
  sourceMtime?: string | null;
  syncedAt?: string | null;
  metadata?: unknown;
  createdAt: string;
  updatedAt: string;
}

export interface DocumentationSnapshotRecord {
  id: string;
  collectionId: string;
  version: number;
  sourceHash: string;
  pageCount: number;
  totalWords: number;
  totalBytes: number;
  toc?: unknown;
  metadata?: unknown;
  createdBy?: string | null;
  createdAt: string;
}

export interface DocumentationCheckRecord {
  id: string;
  collectionId?: string | null;
  checkType: string;
  status: string;
  summary?: unknown;
  issues?: unknown;
  createdBy?: string | null;
  createdAt: string;
}

export interface DocumentationExportRecord {
  id: string;
  collectionId?: string | null;
  format: string;
  status: string;
  checksum: string;
  sizeBytes: number;
  pageCount: number;
  payload?: unknown;
  storageObjectId?: string | null;
  createdBy?: string | null;
  createdAt: string;
}

export interface ReleaseFreezeRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  scope?: unknown;
  reason?: string | null;
  lockedAt: string;
  lockedBy?: string | null;
  releasedAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReleaseQualityGateRecord {
  id: string;
  code: string;
  title: string;
  category: string;
  severity: string;
  isEnabled: boolean;
  config?: unknown;
  lastStatus?: string | null;
  lastRunAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReleaseGateRunRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  rootDir?: string | null;
  trigger: string;
  summary?: unknown;
  results?: unknown;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  createdBy?: string | null;
}

export interface ReleaseBlockerRecord {
  id: string;
  runId?: string | null;
  code: string;
  severity: string;
  status: string;
  source?: string | null;
  message: string;
  details?: unknown;
  acceptedReason?: string | null;
  resolvedAt?: string | null;
  resolvedBy?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReleaseCandidateReportRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  gateRunId?: string | null;
  summary?: unknown;
  markdown?: string | null;
  artifactName?: string | null;
  artifactSha256?: string | null;
  createdBy?: string | null;
  createdAt: string;
}


export interface IntegrationTestSuiteRecord {
  id: string;
  code: string;
  title: string;
  category: string;
  description?: string | null;
  isActive: boolean;
  metadata?: unknown;
  createdAt: string;
  updatedAt: string;
}

export interface IntegrationTestCaseRecord {
  id: string;
  suiteId: string;
  code: string;
  title: string;
  target: string;
  kind: string;
  provider?: string | null;
  priority: number;
  isActive: boolean;
  timeoutMs: number;
  definition?: unknown;
  expected?: unknown;
  lastStatus?: string | null;
  lastRunAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface IntegrationTestRunRecord {
  id: string;
  suiteId?: string | null;
  caseId?: string | null;
  version: string;
  channel: string;
  trigger: string;
  status: string;
  rootDir?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  durationMs?: number | null;
  summary?: unknown;
  environment?: unknown;
  createdBy?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface IntegrationTestStepRunRecord {
  id: string;
  runId: string;
  caseId?: string | null;
  stepIndex: number;
  code: string;
  kind: string;
  status: string;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  request?: unknown;
  response?: unknown;
  assertion?: unknown;
  error?: string | null;
}

export interface IntegrationTestCoverageRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  source: string;
  summary?: unknown;
  areas?: unknown;
  missing?: unknown;
  createdBy?: string | null;
  createdAt: string;
}

export interface SecurityAuditPolicyRecord {
  id: string;
  code: string;
  title: string;
  category: string;
  severity: string;
  isEnabled: boolean;
  config?: unknown;
  lastStatus?: string | null;
  lastRunAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface SecurityAuditRunRecord {
  id: string;
  version: string;
  channel: string;
  trigger: string;
  status: string;
  summary?: unknown;
  results?: unknown;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  createdBy?: string | null;
  createdAt: string;
}

export interface SecurityAuditFindingRecord {
  id: string;
  runId?: string | null;
  code: string;
  category: string;
  severity: string;
  status: string;
  source?: string | null;
  subjectType?: string | null;
  subjectId?: string | null;
  message: string;
  evidence?: unknown;
  remediation?: string | null;
  resolvedAt?: string | null;
  resolvedBy?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface SecurityRiskSnapshotRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  score: number;
  summary?: unknown;
  findingsBySeverity?: unknown;
  recommendations?: unknown;
  createdBy?: string | null;
  createdAt: string;
}


export interface ReleaseApiFreezeRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  sourcePath: string;
  sourceSha256: string;
  openApiVersion: string;
  operationCount: number;
  operations?: unknown;
  payload?: unknown;
  notes?: string | null;
  createdBy?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReleaseCompatibilityTargetRecord {
  id: string;
  code: string;
  kind: string;
  name: string;
  versionRange: string;
  required: boolean;
  status: string;
  notes?: string | null;
  checks?: unknown;
  lastCheckedAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReleaseCompatibilityMatrixRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  summary?: unknown;
  targets?: unknown;
  createdBy?: string | null;
  createdAt: string;
}

export interface ReleaseUpgradePlanRecord {
  id: string;
  fromVersion: string;
  toVersion: string;
  channel: string;
  status: string;
  steps?: unknown;
  risks?: unknown;
  checks?: unknown;
  payload?: unknown;
  createdBy?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReleaseUpgradeRunRecord {
  id: string;
  planId: string;
  mode: string;
  status: string;
  rootDir?: string | null;
  results?: unknown;
  logs?: unknown;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  createdBy?: string | null;
  createdAt: string;
}

export interface ReleaseUpgradeCheckRecord {
  id: string;
  version: string;
  channel: string;
  code: string;
  status: string;
  severity: string;
  message: string;
  details?: unknown;
  createdBy?: string | null;
  createdAt: string;
}

export interface ReleaseCandidateReadinessRunRecord {
  id: string;
  version: string;
  channel: string;
  status: string;
  rootDir?: string | null;
  summary?: unknown;
  sections?: unknown;
  startedAt: string;
  finishedAt?: string | null;
  durationMs?: number | null;
  createdBy?: string | null;
  createdAt: string;
}
