import type { AdminAccountRecord, AdminSessionRecord, ApiKeyRecord, BotAccountRecord, BotInboundUpdateRecord, BotOutboundMessageRecord, BotRuntimeStatusRecord, CajeerClientOptions, Community, CommunityExportPayload, HealthReport, IssuedApiKey, LoginResult, InteractionCommandRecord, InteractionEventRecord, InteractionMenuRecord, CallbackActionRecord, ConversationFlowRecord, ExtensionPermissionGrantRecord, ExtensionRecord, ExtensionRuntimeEventRecord, ExtensionStorageItemRecord, LocaleCatalogRecord, MessageButtonLayoutRecord, MessageFormDefinitionRecord, MessageFormSubmissionRecord, MessageRenderResult, MessageTemplateRecord, RoleRecord, RuntimeConfigSummary, SecretRecord, UserRecord, EventTopicRecord, EventRecordRecord, EventSubscriptionRecord, EventSubscriptionDeliveryRecord, EventDeadLetterRecord, RegistrySourceRecord, RegistryPackageRecord, UpdatePlanRecord, UpdateHistoryRecord, ScheduledJobRecord, ScheduledJobRunRecord, MaintenanceSnapshotRecord, DebugSessionRecord, DebugEventRecord, BotScenarioRecord, BotScenarioRunRecord, StorageObjectRecord, BackupArtifactRecord, BackupRestoreRunRecord, DeploymentTargetRecord, DeploymentCheckRecord, DeploymentRunRecord, ReleaseArtifactRecord, ApiContractSnapshotRecord, ApiContractCheckRecord, SdkGenerationRunRecord, ApiDeprecationRecord, DocumentationCollectionRecord, DocumentationPageRecord, DocumentationSnapshotRecord, DocumentationCheckRecord, DocumentationExportRecord, ReleaseFreezeRecord, ReleaseQualityGateRecord, ReleaseGateRunRecord, ReleaseBlockerRecord, ReleaseCandidateReportRecord, IntegrationTestSuiteRecord, IntegrationTestCaseRecord, IntegrationTestRunRecord, IntegrationTestStepRunRecord, IntegrationTestCoverageRecord, SecurityAuditPolicyRecord, SecurityAuditRunRecord, SecurityAuditFindingRecord, SecurityRiskSnapshotRecord, ReleaseApiFreezeRecord, ReleaseCompatibilityTargetRecord, ReleaseCompatibilityMatrixRecord, ReleaseUpgradePlanRecord, ReleaseUpgradeRunRecord, ReleaseCandidateReadinessRunRecord } from './types.js';

export class CajeerClient {
  constructor(private readonly options: CajeerClientOptions) {}

  health(): Promise<HealthReport> {
    return this.request('/api/health');
  }

  issueToken(input: { adminToken?: string; scopes?: string[]; ttlSeconds?: number }): Promise<{ token: string; expiresAt: string }> {
    return this.request('/api/auth/token', { method: 'POST', body: input });
  }

  bootstrapAdmin(input: { adminToken?: string; email: string; password: string; displayName?: string; scopes?: string[] }): Promise<AdminAccountRecord> {
    return this.request('/api/auth/admin/bootstrap', { method: 'POST', body: input });
  }

  login(input: { email: string; password: string; code?: string; recoveryCode?: string }): Promise<LoginResult> {
    return this.request('/api/auth/login', { method: 'POST', body: input });
  }

  refresh(refreshToken: string): Promise<Omit<LoginResult, 'account'>> {
    return this.request('/api/auth/refresh', { method: 'POST', body: { refreshToken } });
  }

  logout(refreshToken: string): Promise<{ revoked: boolean }> {
    return this.request('/api/auth/logout', { method: 'POST', body: { refreshToken } });
  }

  adminAccounts(): Promise<AdminAccountRecord[]> {
    return this.request('/api/auth/admin-accounts');
  }

  sessions(): Promise<AdminSessionRecord[]> {
    return this.request('/api/auth/sessions');
  }

  revokeSession(id: string): Promise<AdminSessionRecord> {
    return this.request(`/api/auth/sessions/${id}/revoke`, { method: 'POST' });
  }

  setupTwoFactor(): Promise<{ id: string; secret: string; otpauthUri: string }> {
    return this.request('/api/auth/2fa/setup', { method: 'POST' });
  }

  confirmTwoFactor(code: string): Promise<{ enabled: boolean; recoveryCodes: string[] }> {
    return this.request('/api/auth/2fa/confirm', { method: 'POST', body: { code } });
  }

  disableTwoFactor(input: { password: string; code?: string; recoveryCode?: string }): Promise<{ enabled: boolean }> {
    return this.request('/api/auth/2fa/disable', { method: 'POST', body: input });
  }

  rotateRecoveryCodes(input: { password: string; code?: string; recoveryCode?: string }): Promise<{ recoveryCodes: string[] }> {
    return this.request('/api/auth/2fa/recovery-codes/rotate', { method: 'POST', body: input });
  }

  securityHardening(): Promise<unknown> {
    return this.request('/api/security/hardening');
  }

  rateLimitViolations(take?: number): Promise<unknown[]> {
    const query = take ? `?take=${take}` : '';
    return this.request(`/api/security/rate-limit-violations${query}`);
  }

  me(): Promise<unknown> {
    return this.request('/api/auth/me');
  }

  apiKeys(): Promise<ApiKeyRecord[]> {
    return this.request('/api/auth/api-keys');
  }

  createApiKey(input: { title: string; scopes: string[]; expiresAt?: string }): Promise<IssuedApiKey> {
    return this.request('/api/auth/api-keys', { method: 'POST', body: input });
  }

  revokeApiKey(id: string): Promise<ApiKeyRecord> {
    return this.request(`/api/auth/api-keys/${id}/revoke`, { method: 'POST' });
  }

  communities(): Promise<Community[]> {
    return this.request('/api/communities');
  }

  createCommunity(input: { slug: string; title: string; language?: string }): Promise<Community> {
    return this.request('/api/communities', { method: 'POST', body: input, idempotencyKey: `community:${input.slug}` });
  }

  users(communityId: string): Promise<UserRecord[]> {
    return this.request(`/api/communities/${communityId}/users`);
  }

  createUser(communityId: string, input: { provider: 'telegram' | 'vk' | 'discord'; externalId: string; username?: string; displayName?: string; locale?: string }): Promise<UserRecord> {
    return this.request(`/api/communities/${communityId}/users`, { method: 'POST', body: input });
  }

  roles(communityId: string): Promise<RoleRecord[]> {
    return this.request(`/api/communities/${communityId}/roles`);
  }

  createRole(communityId: string, input: { code: string; title: string; priority?: number; scopes?: string[] }): Promise<RoleRecord> {
    return this.request(`/api/communities/${communityId}/roles`, { method: 'POST', body: input });
  }

  setRolePermissions(communityId: string, roleId: string, scopes: string[]): Promise<RoleRecord> {
    return this.request(`/api/communities/${communityId}/roles/${roleId}/permissions`, { method: 'PUT', body: { scopes } });
  }

  botAccounts(communityId: string): Promise<BotAccountRecord[]> {
    return this.request(`/api/communities/${communityId}/bot-accounts`);
  }

  createBotAccount(communityId: string, input: { provider: 'telegram' | 'vk' | 'discord'; mode?: 'polling' | 'webhook'; externalId?: string; username?: string; tokenRef?: string; isActive?: boolean }): Promise<BotAccountRecord> {
    return this.request(`/api/communities/${communityId}/bot-accounts`, { method: 'POST', body: input });
  }



  botRuntimeStatus(communityId: string, accountId?: string): Promise<BotRuntimeStatusRecord[]> {
    const query = accountId ? `?accountId=${encodeURIComponent(accountId)}` : '';
    return this.request(`/api/communities/${communityId}/bot-runtime/status${query}`);
  }

  botInboundUpdates(communityId: string, input: { accountId?: string; status?: string; take?: number } = {}): Promise<BotInboundUpdateRecord[]> {
    const params = new URLSearchParams();
    if (input.accountId) params.set('accountId', input.accountId);
    if (input.status) params.set('status', input.status);
    if (input.take) params.set('take', String(input.take));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/communities/${communityId}/bot-runtime/inbound-updates${query}`);
  }

  botOutboundMessages(communityId: string, input: { accountId?: string; status?: string; take?: number } = {}): Promise<BotOutboundMessageRecord[]> {
    const params = new URLSearchParams();
    if (input.accountId) params.set('accountId', input.accountId);
    if (input.status) params.set('status', input.status);
    if (input.take) params.set('take', String(input.take));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/communities/${communityId}/bot-runtime/outbound-messages${query}`);
  }

  retryBotOutboundMessage(communityId: string, id: string, resetAttempts = false): Promise<BotOutboundMessageRecord> {
    return this.request(`/api/communities/${communityId}/bot-runtime/outbound-messages/${id}/retry`, { method: 'POST', body: { resetAttempts } });
  }

  botWebhookUrl(communityId: string, accountId: string): Promise<{ accountId: string; provider: string; mode: string; url: string }> {
    return this.request(`/api/communities/${communityId}/bot-runtime/accounts/${accountId}/webhook-url`);
  }


  settings(communityId: string): Promise<unknown[]> {
    return this.request(`/api/communities/${communityId}/settings`);
  }

  upsertSetting(communityId: string, key: string, value: unknown, reason?: string): Promise<unknown> {
    return this.request(`/api/communities/${communityId}/settings/${encodeURIComponent(key)}`, { method: 'PUT', body: { value, reason } });
  }

  messageTemplates(locale?: string): Promise<MessageTemplateRecord[]> {
    const query = locale ? `?locale=${encodeURIComponent(locale)}` : '';
    return this.request(`/api/message-templates${query}`);
  }

  upsertMessageTemplate(input: { key: string; locale?: string; body: string; variables?: Record<string, unknown>; buttons?: unknown }): Promise<MessageTemplateRecord> {
    return this.request('/api/message-templates', { method: 'PUT', body: input });
  }

  renderMessageTemplate(key: string, input: { locale?: string; variables?: Record<string, string | number | boolean | null> }): Promise<{ key: string; locale: string; body: string; buttons?: unknown }> {
    return this.request(`/api/message-templates/${encodeURIComponent(key)}/render`, { method: 'POST', body: input });
  }



  localeCatalogs(communityId: string, input: { locale?: string; namespace?: string } = {}): Promise<LocaleCatalogRecord[]> {
    const params = new URLSearchParams();
    if (input.locale) params.set('locale', input.locale);
    if (input.namespace) params.set('namespace', input.namespace);
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/communities/${communityId}/message-core/catalogs${query}`);
  }

  upsertLocaleCatalog(communityId: string, input: { namespace?: string; locale?: string; fallbackLocale?: string | null; messages: Record<string, string> }): Promise<LocaleCatalogRecord> {
    return this.request(`/api/communities/${communityId}/message-core/catalogs`, { method: 'PUT', body: input });
  }

  messageButtonLayouts(communityId: string, input: { code?: string; provider?: 'telegram' | 'vk' | 'discord'; locale?: string } = {}): Promise<MessageButtonLayoutRecord[]> {
    const params = new URLSearchParams();
    if (input.code) params.set('code', input.code);
    if (input.provider) params.set('provider', input.provider);
    if (input.locale) params.set('locale', input.locale);
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/communities/${communityId}/message-core/button-layouts${query}`);
  }

  upsertMessageButtonLayout(communityId: string, input: { code: string; provider?: 'telegram' | 'vk' | 'discord' | null; locale?: string; rows: unknown }): Promise<MessageButtonLayoutRecord> {
    return this.request(`/api/communities/${communityId}/message-core/button-layouts`, { method: 'PUT', body: input });
  }

  messageForms(communityId: string, input: { locale?: string; activeOnly?: boolean } = {}): Promise<MessageFormDefinitionRecord[]> {
    const params = new URLSearchParams();
    if (input.locale) params.set('locale', input.locale);
    if (input.activeOnly) params.set('activeOnly', 'true');
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/communities/${communityId}/message-core/forms${query}`);
  }

  upsertMessageForm(communityId: string, input: { code: string; title: string; locale?: string; fields: unknown; submitTemplateKey?: string | null; successTemplateKey?: string | null; isActive?: boolean }): Promise<MessageFormDefinitionRecord> {
    return this.request(`/api/communities/${communityId}/message-core/forms`, { method: 'PUT', body: input });
  }

  submitMessageForm(communityId: string, code: string, input: { locale?: string; provider?: 'telegram' | 'vk' | 'discord' | null; userExternalId?: string | null; payload: Record<string, unknown> }): Promise<{ form: MessageFormDefinitionRecord; submission: MessageFormSubmissionRecord; validation: unknown }> {
    const query = input.locale ? `?locale=${encodeURIComponent(input.locale)}` : '';
    return this.request(`/api/communities/${communityId}/message-core/forms/${encodeURIComponent(code)}/submit${query}`, { method: 'POST', body: input });
  }

  messageFormSubmissions(communityId: string, input: { formId?: string; take?: number } = {}): Promise<MessageFormSubmissionRecord[]> {
    const params = new URLSearchParams();
    if (input.formId) params.set('formId', input.formId);
    if (input.take) params.set('take', String(input.take));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/communities/${communityId}/message-core/form-submissions${query}`);
  }

  renderMessageCore(communityId: string, input: { locale?: string; provider?: 'telegram' | 'vk' | 'discord'; templateKey?: string; catalogKey?: string; namespace?: string; text?: string; variables?: Record<string, unknown>; buttonLayoutCode?: string; buttons?: unknown; parseMode?: 'plain' | 'markdown' | 'html'; missingVariables?: 'empty' | 'keep' | 'throw' }): Promise<MessageRenderResult> {
    return this.request(`/api/communities/${communityId}/message-core/render`, { method: 'POST', body: input });
  }

  messageRenderLogs(communityId: string, take?: number): Promise<unknown[]> {
    const query = take ? `?take=${take}` : '';
    return this.request(`/api/communities/${communityId}/message-core/render-logs${query}`);
  }

  interactionCommands(communityId: string, provider?: 'telegram' | 'vk' | 'discord'): Promise<InteractionCommandRecord[]> {
    const query = provider ? `?provider=${encodeURIComponent(provider)}` : '';
    return this.request(`/api/communities/${communityId}/interactions/commands${query}`);
  }

  createInteractionCommand(communityId: string, input: Partial<InteractionCommandRecord> & { name: string; description: string }): Promise<InteractionCommandRecord> {
    return this.request(`/api/communities/${communityId}/interactions/commands`, { method: 'POST', body: input });
  }

  interactionMenus(communityId: string, locale?: string): Promise<InteractionMenuRecord[]> {
    const query = locale ? `?locale=${encodeURIComponent(locale)}` : '';
    return this.request(`/api/communities/${communityId}/interactions/menus${query}`);
  }

  createInteractionMenu(communityId: string, input: { code: string; title: string; locale?: string; templateKey?: string | null; buttons: unknown; isActive?: boolean }): Promise<InteractionMenuRecord> {
    return this.request(`/api/communities/${communityId}/interactions/menus`, { method: 'POST', body: input });
  }

  callbackActions(communityId: string): Promise<CallbackActionRecord[]> {
    return this.request(`/api/communities/${communityId}/interactions/callbacks`);
  }

  createCallbackAction(communityId: string, input: { code: string; title: string; handler?: string; scopes?: string[]; payload?: Record<string, unknown>; isActive?: boolean }): Promise<CallbackActionRecord> {
    return this.request(`/api/communities/${encodeURIComponent(communityId)}/interactions/callbacks`, { method: 'POST', body: input });
  }
  createConversationFlow(communityId: string, input: { code: string; title: string; entryCommand?: string; steps: unknown; isActive?: boolean }): Promise<ConversationFlowRecord> {
    return this.request(`/api/communities/${encodeURIComponent(communityId)}/interactions/conversations`, { method: 'POST', body: input });
  }

  conversationFlows(communityId: string): Promise<ConversationFlowRecord[]> {
    return this.request(`/api/communities/${communityId}/interactions/conversations`);
  }

  interactionEvents(communityId: string, take?: number): Promise<InteractionEventRecord[]> {
    const query = take ? `?take=${take}` : '';
    return this.request(`/api/communities/${communityId}/interactions/events${query}`);
  }

  configSummary(): Promise<RuntimeConfigSummary> {
    return this.request('/api/config/summary');
  }

  configValidate(): Promise<{ ok: boolean; issues: Array<{ path: string; message: string }> }> {
    return this.request('/api/config/validate');
  }

  secrets(): Promise<SecretRecord[]> {
    return this.request('/api/secrets');
  }

  setSecret(input: { key: string; value: string; description?: string; reason?: string }): Promise<SecretRecord> {
    return this.request('/api/secrets', { method: 'POST', body: input });
  }

  rotateSecret(key: string, input: { value: string; description?: string; reason?: string }): Promise<SecretRecord> {
    return this.request(`/api/secrets/${encodeURIComponent(key)}/rotate`, { method: 'POST', body: input });
  }

  exportCommunity(communityId: string): Promise<CommunityExportPayload> {
    return this.request(`/api/communities/${communityId}/import-export/export`);
  }

  importCommunity(communityId: string, data: CommunityExportPayload, apply = false): Promise<unknown> {
    return this.request(`/api/communities/${communityId}/import-export/import`, { method: 'POST', body: { mode: apply ? 'apply' : 'dry-run', data } });
  }


  extensions(status?: 'installed' | 'enabled' | 'disabled' | 'failed'): Promise<ExtensionRecord[]> {
    const query = status ? `?status=${encodeURIComponent(status)}` : '';
    return this.request(`/api/extensions${query}`);
  }

  extension(id: string): Promise<ExtensionRecord> {
    return this.request(`/api/extensions/${id}`);
  }

  installExtension(input: { manifest: unknown; signature?: string; autoGrant?: boolean }): Promise<ExtensionRecord> {
    return this.request('/api/extensions/install', { method: 'POST', body: input });
  }

  enableExtension(id: string): Promise<ExtensionRecord> {
    return this.request(`/api/extensions/${id}/enable`, { method: 'POST' });
  }

  disableExtension(id: string): Promise<ExtensionRecord> {
    return this.request(`/api/extensions/${id}/disable`, { method: 'POST' });
  }

  extensionPermissions(id: string): Promise<ExtensionPermissionGrantRecord[]> {
    return this.request(`/api/extensions/${id}/permissions`);
  }

  grantExtensionPermissions(id: string, permissions: string[], reason?: string): Promise<ExtensionPermissionGrantRecord[]> {
    return this.request(`/api/extensions/${id}/permissions/grant`, { method: 'POST', body: { permissions, reason } });
  }

  revokeExtensionPermissions(id: string, permissions: string[], reason?: string): Promise<ExtensionPermissionGrantRecord[]> {
    return this.request(`/api/extensions/${id}/permissions/revoke`, { method: 'POST', body: { permissions, reason } });
  }

  enqueueExtensionEvent(id: string, hook: string, payload: unknown): Promise<{ queued: number; skipped: unknown[] }> {
    return this.request(`/api/extensions/${id}/events`, { method: 'POST', body: { hook, payload } });
  }

  extensionEvents(id: string, status?: string, take?: number): Promise<ExtensionRuntimeEventRecord[]> {
    const params = new URLSearchParams();
    if (status) params.set('status', status);
    if (take) params.set('take', String(take));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/extensions/${id}/events${query}`);
  }

  extensionStorage(id: string, namespace = 'default'): Promise<ExtensionStorageItemRecord[]> {
    return this.request(`/api/extensions/${id}/storage?namespace=${encodeURIComponent(namespace)}`);
  }

  setExtensionStorage(id: string, namespace: string, key: string, value: unknown): Promise<ExtensionStorageItemRecord> {
    return this.request(`/api/extensions/${id}/storage/${encodeURIComponent(namespace)}/${encodeURIComponent(key)}`, { method: 'PUT', body: { value } });
  }

  outbox(status?: string): Promise<unknown[]> {
    const query = status ? `?status=${encodeURIComponent(status)}` : '';
    return this.request(`/api/outbox${query}`);
  }

  retryOutbox(id: string): Promise<unknown> {
    return this.request(`/api/outbox/${id}/retry`, { method: 'POST' });
  }


  eventTopics(): Promise<EventTopicRecord[]> {
    return this.request('/api/events/topics');
  }

  upsertEventTopic(name: string, input: { description?: string; schema?: Record<string, unknown>; isActive?: boolean; retentionDays?: number }): Promise<EventTopicRecord> {
    return this.request(`/api/events/topics/${encodeURIComponent(name)}`, { method: 'PUT', body: input });
  }

  publishEvent(input: { id?: string; topic?: string; type: string; source?: string; communityId?: string; aggregateType?: string; aggregateId?: string; payload: unknown; metadata?: Record<string, unknown>; headers?: Record<string, unknown>; fanoutToOutbox?: boolean }): Promise<{ accepted: boolean; event: EventRecordRecord; matchedSubscriptions: number; outboxId?: string | null }> {
    return this.request('/api/events/publish', { method: 'POST', body: input, idempotencyKey: input.id ? `event:${input.id}` : undefined });
  }

  eventRecords(input: { topic?: string; type?: string; communityId?: string; aggregateType?: string; aggregateId?: string; source?: string; take?: number } = {}): Promise<EventRecordRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/events/records${query}`);
  }

  replayEvent(id: string): Promise<{ replayed: boolean; eventId: string; matchedSubscriptions: number }> {
    return this.request(`/api/events/records/${id}/replay`, { method: 'POST' });
  }

  eventSubscriptions(): Promise<EventSubscriptionRecord[]> {
    return this.request('/api/events/subscriptions');
  }

  createEventSubscription(input: { name: string; topicPattern: string; targetType: 'outbox' | 'webhook_endpoint' | 'extension_hook' | 'log'; targetRef: string; filters?: Record<string, unknown>; isActive?: boolean; maxAttempts?: number }): Promise<EventSubscriptionRecord> {
    return this.request('/api/events/subscriptions', { method: 'POST', body: input });
  }

  updateEventSubscription(id: string, input: Partial<{ topicPattern: string; targetType: 'outbox' | 'webhook_endpoint' | 'extension_hook' | 'log'; targetRef: string; filters: Record<string, unknown>; isActive: boolean; maxAttempts: number }>): Promise<EventSubscriptionRecord> {
    return this.request(`/api/events/subscriptions/${id}`, { method: 'PUT', body: input });
  }

  eventDeliveries(input: { subscriptionId?: string; status?: string; take?: number } = {}): Promise<EventSubscriptionDeliveryRecord[]> {
    const params = new URLSearchParams();
    if (input.subscriptionId) params.set('subscriptionId', input.subscriptionId);
    if (input.status) params.set('status', input.status);
    if (input.take) params.set('take', String(input.take));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/events/deliveries${query}`);
  }

  retryEventDelivery(id: string, resetAttempts = false): Promise<EventSubscriptionDeliveryRecord> {
    return this.request(`/api/events/deliveries/${id}/retry`, { method: 'POST', body: { resetAttempts } });
  }

  eventDeadLetters(status?: string, take?: number): Promise<EventDeadLetterRecord[]> {
    const params = new URLSearchParams();
    if (status) params.set('status', status);
    if (take) params.set('take', String(take));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/events/dead-letter${query}`);
  }

  resolveEventDeadLetter(id: string): Promise<EventDeadLetterRecord> {
    return this.request(`/api/events/dead-letter/${id}/resolve`, { method: 'POST' });
  }



  registrySources(): Promise<RegistrySourceRecord[]> {
    return this.request('/api/registry/sources');
  }

  createRegistrySource(input: { name: string; url: string; type?: string; authSecretRef?: string; publicKey?: string; isActive?: boolean }): Promise<RegistrySourceRecord> {
    return this.request('/api/registry/sources', { method: 'POST', body: input });
  }

  syncRegistrySource(id: string): Promise<{ ok: boolean; sourceId: string; packages: number; versions: number }> {
    return this.request(`/api/registry/sources/${encodeURIComponent(id)}/sync`, { method: 'POST' });
  }

  registryPackages(input: { sourceId?: string; kind?: string; q?: string; take?: number } = {}): Promise<RegistryPackageRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/registry/packages${query}`);
  }

  registryPackage(name: string, sourceId?: string): Promise<RegistryPackageRecord> {
    const query = sourceId ? `?sourceId=${encodeURIComponent(sourceId)}` : '';
    return this.request(`/api/registry/packages/${encodeURIComponent(name)}${query}`);
  }

  installRegistryPackage(name: string, input: { sourceId?: string; version?: string; channel?: 'stable' | 'lts' | 'beta' | 'alpha' | 'security'; autoGrant?: boolean } = {}): Promise<ExtensionRecord> {
    return this.request(`/api/registry/packages/${encodeURIComponent(name)}/install`, { method: 'POST', body: input });
  }

  updatePlans(input: { targetType?: string; status?: string; take?: number } = {}): Promise<UpdatePlanRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/updates/plans${query}`);
  }

  planCoreUpdate(input: { packageName?: string; currentVersion: string; sourceId?: string; channel?: 'stable' | 'lts' | 'beta' | 'alpha' | 'security'; includePrerelease?: boolean }): Promise<UpdatePlanRecord> {
    return this.request('/api/updates/plans/core', { method: 'POST', body: input });
  }

  planExtensionUpdate(extensionId: string, input: { sourceId?: string; channel?: 'stable' | 'lts' | 'beta' | 'alpha' | 'security'; includePrerelease?: boolean } = {}): Promise<UpdatePlanRecord> {
    return this.request(`/api/updates/plans/extensions/${encodeURIComponent(extensionId)}`, { method: 'POST', body: input });
  }

  applyUpdatePlan(id: string, input: { dryRun?: boolean; autoGrant?: boolean; discardPending?: boolean } = {}): Promise<UpdatePlanRecord | { dryRun: true; plan: UpdatePlanRecord; steps: unknown }> {
    return this.request(`/api/updates/plans/${encodeURIComponent(id)}/apply`, { method: 'POST', body: input });
  }

  rollbackUpdatePlan(id: string, discardPending = false): Promise<UpdatePlanRecord> {
    return this.request(`/api/updates/plans/${encodeURIComponent(id)}/rollback`, { method: 'POST', body: { discardPending } });
  }

  updateHistory(take?: number): Promise<UpdateHistoryRecord[]> {
    const query = take ? `?take=${take}` : '';
    return this.request(`/api/updates/history${query}`);
  }


  scheduledJobs(input: { enabled?: boolean; type?: string; q?: string; take?: number } = {}): Promise<ScheduledJobRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/jobs${query}`);
  }

  scheduledJob(idOrCode: string): Promise<ScheduledJobRecord & { runs?: ScheduledJobRunRecord[] }> {
    return this.request(`/api/jobs/${encodeURIComponent(idOrCode)}`);
  }

  upsertScheduledJob(input: { code: string; title: string; type?: 'built_in' | 'event_publish' | 'http'; handler?: string; schedule?: string; timezone?: string; payload?: Record<string, unknown>; isEnabled?: boolean; maxAttempts?: number; timeoutSeconds?: number }): Promise<ScheduledJobRecord> {
    return this.request('/api/jobs', { method: 'PUT', body: input });
  }

  enableScheduledJob(idOrCode: string): Promise<ScheduledJobRecord> {
    return this.request(`/api/jobs/${encodeURIComponent(idOrCode)}/enable`, { method: 'POST' });
  }

  disableScheduledJob(idOrCode: string): Promise<ScheduledJobRecord> {
    return this.request(`/api/jobs/${encodeURIComponent(idOrCode)}/disable`, { method: 'POST' });
  }

  runScheduledJob(idOrCode: string, input: { resetLock?: boolean; payloadPatch?: Record<string, unknown> } = {}): Promise<ScheduledJobRunRecord> {
    return this.request(`/api/jobs/${encodeURIComponent(idOrCode)}/run`, { method: 'POST', body: input });
  }

  scheduledJobRuns(input: { jobId?: string; status?: string; take?: number } = {}): Promise<ScheduledJobRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/jobs/runs${query}`);
  }

  schedulerQueueSummary(): Promise<Record<string, number>> {
    return this.request('/api/jobs/queue-summary');
  }

  maintenanceSnapshots(input: { kind?: string; status?: string; take?: number } = {}): Promise<MaintenanceSnapshotRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/jobs/maintenance/snapshots${query}`);
  }


  debugSessions(input: { communityId?: string; status?: string; take?: number } = {}): Promise<DebugSessionRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/devtools/debug/sessions${query}`);
  }

  createDebugSession(input: { communityId?: string; title: string; metadata?: Record<string, unknown> }): Promise<DebugSessionRecord> {
    return this.request('/api/devtools/debug/sessions', { method: 'POST', body: input });
  }

  endDebugSession(id: string): Promise<DebugSessionRecord> {
    return this.request(`/api/devtools/debug/sessions/${encodeURIComponent(id)}/end`, { method: 'POST' });
  }

  debugEvents(input: { sessionId?: string; communityId?: string; level?: string; category?: string; take?: number } = {}): Promise<DebugEventRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/devtools/debug/events${query}`);
  }

  recordDebugEvent(input: { sessionId?: string; communityId?: string; level?: 'debug' | 'info' | 'warn' | 'error'; category: string; message: string; metadata?: Record<string, unknown> }): Promise<DebugEventRecord> {
    return this.request('/api/devtools/debug/events', { method: 'POST', body: input });
  }

  botScenarios(communityId: string, input: { provider?: 'telegram' | 'vk' | 'discord'; active?: boolean; q?: string; take?: number } = {}): Promise<BotScenarioRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/devtools/communities/${encodeURIComponent(communityId)}/scenarios${query}`);
  }

  botScenario(communityId: string, idOrCode: string): Promise<BotScenarioRecord & { runs?: BotScenarioRunRecord[] }> {
    return this.request(`/api/devtools/communities/${encodeURIComponent(communityId)}/scenarios/${encodeURIComponent(idOrCode)}`);
  }

  upsertBotScenario(communityId: string, input: { code: string; title: string; provider?: 'telegram' | 'vk' | 'discord' | null; accountId?: string | null; definition: unknown; isActive?: boolean }): Promise<BotScenarioRecord> {
    return this.request(`/api/devtools/communities/${encodeURIComponent(communityId)}/scenarios`, { method: 'PUT', body: input });
  }

  runBotScenario(communityId: string, idOrCode: string, input: { accountId?: string; provider?: 'telegram' | 'vk' | 'discord'; dryRun?: boolean } = {}): Promise<BotScenarioRunRecord> {
    return this.request(`/api/devtools/communities/${encodeURIComponent(communityId)}/scenarios/${encodeURIComponent(idOrCode)}/run`, { method: 'POST', body: input });
  }

  botScenarioRuns(communityId: string, input: { scenarioId?: string; status?: string; take?: number } = {}): Promise<BotScenarioRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/devtools/communities/${encodeURIComponent(communityId)}/scenario-runs${query}`);
  }

  simulateProviderUpdate(communityId: string, input: { accountId: string; provider: 'telegram' | 'vk' | 'discord'; type?: 'message' | 'command' | 'callback' | 'form'; text?: string; command?: string; callbackCode?: string; userExternalId?: string; chatExternalId?: string; payload?: Record<string, unknown> }): Promise<BotInboundUpdateRecord> {
    return this.request(`/api/devtools/communities/${encodeURIComponent(communityId)}/simulate-update`, { method: 'POST', body: input });
  }


  storageObjects(input: { status?: string; refType?: string; refId?: string; take?: number } = {}): Promise<StorageObjectRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/storage/objects${query}`);
  }

  storageObject(id: string): Promise<StorageObjectRecord> {
    return this.request(`/api/storage/objects/${encodeURIComponent(id)}`);
  }

  deleteStorageObject(id: string, hard = false): Promise<StorageObjectRecord> {
    return this.request(`/api/storage/objects/${encodeURIComponent(id)}`, { method: 'DELETE', body: { hard } });
  }

  backups(input: { communityId?: string; status?: string; take?: number } = {}): Promise<BackupArtifactRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/backups${query}`);
  }

  backup(id: string): Promise<BackupArtifactRecord> {
    return this.request(`/api/backups/${encodeURIComponent(id)}`);
  }

  createCommunityBackup(communityId: string, input: { title?: string; key?: string; metadata?: Record<string, unknown> } = {}): Promise<BackupArtifactRecord> {
    return this.request(`/api/communities/${encodeURIComponent(communityId)}/backups`, { method: 'POST', body: input });
  }

  verifyBackup(id: string): Promise<unknown> {
    return this.request(`/api/backups/${encodeURIComponent(id)}/verify`, { method: 'POST' });
  }

  restoreBackup(id: string, input: { targetCommunityId?: string; apply?: boolean; strategy?: 'merge' } = {}): Promise<BackupRestoreRunRecord> {
    return this.request(`/api/backups/${encodeURIComponent(id)}/restore`, { method: 'POST', body: input });
  }

  backupRestoreRuns(input: { artifactId?: string; communityId?: string; status?: string; take?: number } = {}): Promise<BackupRestoreRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/backups/restore-runs${query}`);
  }


  deploymentTargets(input: { kind?: string; environment?: string; status?: string; take?: number } = {}): Promise<DeploymentTargetRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/deployment/targets${query}`);
  }

  upsertDeploymentTarget(input: { code: string; title: string; kind?: 'docker_compose' | 'systemd' | 'custom'; environment?: string; baseUrl?: string | null; composeFiles?: string[]; envFile?: string | null; metadata?: Record<string, unknown> }): Promise<DeploymentTargetRecord> {
    return this.request('/api/deployment/targets', { method: 'PUT', body: input });
  }

  runDeploymentChecks(input: { targetId?: string; rootDir?: string; composeFiles?: string[] } = {}): Promise<{ status: string; checks: DeploymentCheckRecord[] }> {
    if (input.targetId) return this.request(`/api/deployment/targets/${encodeURIComponent(input.targetId)}/check`, { method: 'POST', body: { rootDir: input.rootDir, composeFiles: input.composeFiles } });
    return this.request('/api/deployment/checks/run', { method: 'POST', body: { rootDir: input.rootDir, composeFiles: input.composeFiles } });
  }

  deploymentChecks(input: { targetId?: string; status?: string; severity?: string; take?: number } = {}): Promise<DeploymentCheckRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/deployment/checks${query}`);
  }

  createDeploymentRun(input: { targetId?: string | null; action: string; status?: string; logs?: unknown[]; metadata?: Record<string, unknown> }): Promise<DeploymentRunRecord> {
    return this.request('/api/deployment/runs', { method: 'POST', body: input });
  }

  finishDeploymentRun(id: string, input: { status: 'success' | 'failed' | 'cancelled'; error?: string | null; logs?: unknown[]; metadata?: Record<string, unknown> }): Promise<DeploymentRunRecord> {
    return this.request(`/api/deployment/runs/${encodeURIComponent(id)}/finish`, { method: 'POST', body: input });
  }

  renderProductionEnv(input: Record<string, unknown> = {}): Promise<{ filename: string; content: string; generatedAt: string }> {
    return this.request('/api/deployment/env/render', { method: 'POST', body: input });
  }

  validateProductionEnv(envText: string): Promise<{ valid: boolean; errors: string[]; warnings: string[] }> {
    return this.request('/api/deployment/env/validate', { method: 'POST', body: { envText } });
  }

  renderNginxConfig(input: Record<string, unknown>): Promise<{ filename: string; content: string; generatedAt: string }> {
    return this.request('/api/deployment/nginx/render', { method: 'POST', body: input });
  }

  renderSystemdUnits(input: { workdir?: string; envFile?: string } = {}): Promise<{ files: Record<string, string>; generatedAt: string }> {
    return this.request('/api/deployment/systemd/render', { method: 'POST', body: input });
  }

  buildReleaseManifest(input: { product?: string; version: string; channel?: 'alpha' | 'beta' | 'rc' | 'stable' | 'lts' | 'security'; commit?: string; createdBy?: string; files: Array<{ path: string; sizeBytes: number; sha256: string; contentType?: string }> }): Promise<{ artifact: ReleaseArtifactRecord; manifest: unknown }> {
    return this.request('/api/deployment/release-manifest', { method: 'POST', body: input });
  }

  releaseArtifacts(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<ReleaseArtifactRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/deployment/release-artifacts${query}`);
  }


  contractSnapshots(input: { version?: string; baselineOnly?: boolean; take?: number } = {}): Promise<ApiContractSnapshotRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/contracts/snapshots${query}`);
  }

  contractSnapshot(id: string): Promise<ApiContractSnapshotRecord> {
    return this.request(`/api/contracts/snapshots/${encodeURIComponent(id)}`);
  }

  createContractSnapshot(input: { sourcePath?: string; version?: string; makeBaseline?: boolean } = {}): Promise<ApiContractSnapshotRecord> {
    return this.request('/api/contracts/snapshots', { method: 'POST', body: input });
  }

  runContractCheck(input: { snapshotId?: string; baselineId?: string; sourcePath?: string; sdkSourcePath?: string; failOnBreaking?: boolean; failOnUnsecured?: boolean; minSdkCoveragePercent?: number } = {}): Promise<{ check: ApiContractCheckRecord; snapshot: ApiContractSnapshotRecord; baseline?: ApiContractSnapshotRecord | null; diff?: unknown; sdkCoverage: unknown; issues: unknown[] }> {
    return this.request('/api/contracts/checks/run', { method: 'POST', body: input });
  }

  contractChecks(input: { status?: string; checkType?: string; snapshotId?: string; take?: number } = {}): Promise<ApiContractCheckRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/contracts/checks${query}`);
  }

  generateSdkFromContract(input: { snapshotId?: string; sourcePath?: string; sdkSourcePath?: string; outputMode?: 'database' | 'file'; outputDir?: string } = {}): Promise<{ run: SdkGenerationRunRecord; coverage: unknown; files: unknown[] }> {
    return this.request('/api/contracts/sdk/generate', { method: 'POST', body: input });
  }

  sdkGenerationRuns(input: { status?: string; snapshotId?: string; take?: number } = {}): Promise<SdkGenerationRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/contracts/sdk/runs${query}`);
  }

  apiDeprecations(input: { status?: string; take?: number } = {}): Promise<ApiDeprecationRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/contracts/deprecations${query}`);
  }

  upsertApiDeprecation(input: { operationId: string; method: string; path: string; status?: 'planned' | 'deprecated' | 'removed' | 'cancelled'; reason?: string | null; replacement?: string | null; deprecatedSince?: string | null; removeAfter?: string | null; metadata?: Record<string, unknown> }): Promise<ApiDeprecationRecord> {
    return this.request('/api/contracts/deprecations', { method: 'PUT', body: input });
  }


  documentationCollections(input: { code?: string; locale?: string; sourceType?: string; take?: number } = {}): Promise<DocumentationCollectionRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/docs/collections${query}`);
  }

  upsertDocumentationCollection(input: { code: string; title: string; locale?: string; sourceType?: 'local_wiki' | 'gitflic_wiki' | 'manual'; sourcePath?: string; sourceUrl?: string | null; metadata?: Record<string, unknown> }): Promise<DocumentationCollectionRecord> {
    return this.request('/api/docs/collections', { method: 'PUT', body: input });
  }

  syncDocumentationCollection(collectionId: string, input: { rootDir?: string; includeDrafts?: boolean; createSnapshot?: boolean } = {}): Promise<unknown> {
    return this.request(`/api/docs/collections/${encodeURIComponent(collectionId)}/sync`, { method: 'POST', body: input });
  }

  documentationPages(input: { collectionId?: string; collectionCode?: string; locale?: string; status?: string; tag?: string; take?: number } = {}): Promise<DocumentationPageRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/docs/pages${query}`);
  }

  documentationPage(idOrSlug: string, collectionCode?: string): Promise<DocumentationPageRecord> {
    const query = collectionCode ? `?collectionCode=${encodeURIComponent(collectionCode)}` : '';
    return this.request(`/api/docs/pages/${encodeURIComponent(idOrSlug)}${query}`);
  }

  searchDocumentation(input: { q: string; collectionCode?: string; locale?: string; take?: number }): Promise<Array<{ slug: string; title: string; path: string; excerpt: string; score: number; matches: string[]; page?: DocumentationPageRecord }>> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    return this.request(`/api/docs/search?${params.toString()}`);
  }

  runDocumentationCheck(input: { collectionCode?: string; rootDir?: string; includeDrafts?: boolean } = {}): Promise<{ check: DocumentationCheckRecord; status: string; summary: unknown; issues: unknown[] }> {
    return this.request('/api/docs/checks/run', { method: 'POST', body: input });
  }

  documentationChecks(input: { collectionId?: string; status?: string; checkType?: string; take?: number } = {}): Promise<DocumentationCheckRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/docs/checks${query}`);
  }

  documentationSnapshots(input: { collectionId?: string; collectionCode?: string; take?: number } = {}): Promise<DocumentationSnapshotRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/docs/snapshots${query}`);
  }

  createDocumentationExport(input: { collectionCode?: string; rootDir?: string; format?: 'json_bundle' | 'markdown_index'; includeBody?: boolean } = {}): Promise<DocumentationExportRecord> {
    return this.request('/api/docs/exports', { method: 'POST', body: input });
  }

  documentationExports(input: { collectionId?: string; format?: string; take?: number } = {}): Promise<DocumentationExportRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/docs/exports${query}`);
  }


  releaseGateSummary(): Promise<{ freeze?: ReleaseFreezeRecord | null; lastRun?: ReleaseGateRunRecord | null; openBlockers: number; enabledGates: number; defaultGateCount: number }> {
    return this.request('/api/release-gates/summary');
  }

  releaseFreezes(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<ReleaseFreezeRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-gates/freezes${query}`);
  }

  upsertReleaseFreeze(input: { version?: string; channel?: 'alpha' | 'beta' | 'rc' | 'stable' | 'lts' | 'security'; status?: 'open' | 'locked' | 'released' | 'cancelled'; reason?: string | null; scope?: Record<string, unknown> } = {}): Promise<ReleaseFreezeRecord> {
    return this.request('/api/release-gates/freezes', { method: 'PUT', body: input });
  }

  seedDefaultReleaseGates(): Promise<{ count: number; gates: ReleaseQualityGateRecord[] }> {
    return this.request('/api/release-gates/quality-gates/defaults', { method: 'POST' });
  }

  releaseQualityGates(input: { category?: string; enabled?: boolean; take?: number } = {}): Promise<ReleaseQualityGateRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-gates/quality-gates${query}`);
  }

  upsertReleaseQualityGate(input: { code: string; title: string; category: string; severity?: 'info' | 'warning' | 'error' | 'blocker'; isEnabled?: boolean; config?: Record<string, unknown> }): Promise<ReleaseQualityGateRecord> {
    return this.request('/api/release-gates/quality-gates', { method: 'PUT', body: input });
  }

  runReleaseGates(input: { version?: string; channel?: 'alpha' | 'beta' | 'rc' | 'stable' | 'lts' | 'security'; rootDir?: string; trigger?: string; persistBlockers?: boolean; useDatabaseGates?: boolean } = {}): Promise<{ run: ReleaseGateRunRecord; report: unknown }> {
    return this.request('/api/release-gates/runs', { method: 'POST', body: input });
  }

  releaseGateRuns(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<ReleaseGateRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-gates/runs${query}`);
  }

  releaseBlockers(input: { status?: string; severity?: string; runId?: string; take?: number } = {}): Promise<ReleaseBlockerRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-gates/blockers${query}`);
  }

  resolveReleaseBlocker(id: string, input: { status?: 'open' | 'accepted' | 'fixed' | 'false_positive'; reason?: string | null } = {}): Promise<ReleaseBlockerRecord> {
    return this.request(`/api/release-gates/blockers/${encodeURIComponent(id)}`, { method: 'PUT', body: input });
  }

  createReleaseCandidateReport(input: { version?: string; channel?: 'alpha' | 'beta' | 'rc' | 'stable' | 'lts' | 'security'; gateRunId?: string; rootDir?: string; createRunIfMissing?: boolean } = {}): Promise<{ report: ReleaseCandidateReportRecord; summary: unknown; markdown: string }> {
    return this.request('/api/release-gates/reports', { method: 'POST', body: input });
  }

  releaseCandidateReports(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<ReleaseCandidateReportRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-gates/reports${query}`);
  }


  integrationTestSummary(): Promise<{ suites: number; cases: number; lastRun?: IntegrationTestRunRecord | null; failedRuns: number; coverage?: IntegrationTestCoverageRecord | null; defaultSuiteCount: number }> {
    return this.request('/api/integration-tests/summary');
  }

  seedDefaultIntegrationTests(): Promise<{ suites: number; cases: number }> {
    return this.request('/api/integration-tests/defaults', { method: 'POST' });
  }

  integrationTestSuites(input: { category?: string; active?: boolean; take?: number } = {}): Promise<IntegrationTestSuiteRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/integration-tests/suites${query}`);
  }

  upsertIntegrationTestSuite(input: { code: string; title: string; category?: string; description?: string | null; isActive?: boolean; metadata?: Record<string, unknown> }): Promise<IntegrationTestSuiteRecord> {
    return this.request('/api/integration-tests/suites', { method: 'PUT', body: input });
  }

  integrationTestCases(input: { suiteCode?: string; target?: string; kind?: string; provider?: string; active?: boolean; take?: number } = {}): Promise<IntegrationTestCaseRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/integration-tests/cases${query}`);
  }

  upsertIntegrationTestCase(input: { suiteCode: string; code: string; title: string; target?: string; kind?: string; provider?: string | null; priority?: number; isActive?: boolean; timeoutMs?: number; definition?: Record<string, unknown>; expected?: Record<string, unknown> }): Promise<IntegrationTestCaseRecord> {
    return this.request('/api/integration-tests/cases', { method: 'PUT', body: input });
  }

  integrationTestCase(suiteCode: string, caseCode: string): Promise<IntegrationTestCaseRecord> {
    return this.request(`/api/integration-tests/suites/${encodeURIComponent(suiteCode)}/cases/${encodeURIComponent(caseCode)}`);
  }

  runIntegrationTestSuite(suiteCode: string, input: { version?: string; channel?: string; trigger?: string; baseUrl?: string; apiToken?: string; dryRun?: boolean; rootDir?: string } = {}): Promise<{ run: IntegrationTestRunRecord; summary: unknown; cases: unknown[] }> {
    return this.request(`/api/integration-tests/suites/${encodeURIComponent(suiteCode)}/run`, { method: 'POST', body: input });
  }

  runIntegrationTestCase(suiteCode: string, caseCode: string, input: { version?: string; channel?: string; trigger?: string; baseUrl?: string; apiToken?: string; dryRun?: boolean; rootDir?: string } = {}): Promise<{ run: IntegrationTestRunRecord; summary: unknown; steps: IntegrationTestStepRunRecord[] }> {
    return this.request(`/api/integration-tests/suites/${encodeURIComponent(suiteCode)}/cases/${encodeURIComponent(caseCode)}/run`, { method: 'POST', body: input });
  }

  queueIntegrationTestRun(input: { suiteCode?: string; caseCode?: string; version?: string; channel?: string; trigger?: string; rootDir?: string; environment?: Record<string, unknown> } = {}): Promise<IntegrationTestRunRecord> {
    return this.request('/api/integration-tests/runs/queue', { method: 'POST', body: input });
  }

  integrationTestRuns(input: { suiteCode?: string; caseCode?: string; status?: string; version?: string; channel?: string; take?: number } = {}): Promise<IntegrationTestRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/integration-tests/runs${query}`);
  }

  integrationTestRun(id: string): Promise<IntegrationTestRunRecord> {
    return this.request(`/api/integration-tests/runs/${encodeURIComponent(id)}`);
  }

  integrationTestSteps(input: { runId?: string; caseId?: string; status?: string; kind?: string; take?: number } = {}): Promise<IntegrationTestStepRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/integration-tests/steps${query}`);
  }

  calculateIntegrationTestCoverage(input: { version?: string; channel?: string; persist?: boolean } = {}): Promise<{ coverage: IntegrationTestCoverageRecord; summary: unknown; areas: unknown[]; missing: unknown[] }> {
    return this.request('/api/integration-tests/coverage', { method: 'POST', body: input });
  }

  integrationTestCoverage(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<IntegrationTestCoverageRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/integration-tests/coverage${query}`);
  }



  securityAuditSummary(): Promise<{ policies: number; lastRun?: SecurityAuditRunRecord | null; openFindings: number; criticalFindings: number; latestRiskSnapshot?: SecurityRiskSnapshotRecord | null }> {
    return this.request('/api/security/audit/summary');
  }

  seedDefaultSecurityAuditPolicies(): Promise<{ count: number; policies: string[] }> {
    return this.request('/api/security/audit/defaults', { method: 'POST' });
  }

  securityAuditPolicies(input: { category?: string; enabled?: boolean; take?: number } = {}): Promise<SecurityAuditPolicyRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/security/audit/policies${query}`);
  }

  upsertSecurityAuditPolicy(input: { code: string; title: string; category: string; severity?: 'info' | 'warning' | 'error' | 'critical'; isEnabled?: boolean; config?: Record<string, unknown> }): Promise<SecurityAuditPolicyRecord> {
    return this.request('/api/security/audit/policies', { method: 'PUT', body: input });
  }

  runSecurityAudit(input: { version?: string; channel?: string; trigger?: string; persist?: boolean } = {}): Promise<{ run?: SecurityAuditRunRecord; report: unknown }> {
    return this.request('/api/security/audit/runs', { method: 'POST', body: input });
  }

  securityAuditRuns(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<SecurityAuditRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/security/audit/runs${query}`);
  }

  securityAuditRun(id: string): Promise<SecurityAuditRunRecord & { findings?: SecurityAuditFindingRecord[] }> {
    return this.request(`/api/security/audit/runs/${encodeURIComponent(id)}`);
  }

  securityAuditFindings(input: { runId?: string; status?: string; severity?: string; category?: string; subjectType?: string; subjectId?: string; take?: number } = {}): Promise<SecurityAuditFindingRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/security/audit/findings${query}`);
  }

  resolveSecurityAuditFinding(id: string, input: { status?: 'open' | 'accepted' | 'fixed' | 'false_positive'; reason?: string | null } = {}): Promise<SecurityAuditFindingRecord> {
    return this.request(`/api/security/audit/findings/${encodeURIComponent(id)}`, { method: 'PUT', body: input });
  }

  securityRiskSnapshots(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<SecurityRiskSnapshotRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/security/risk-snapshots${query}`);
  }


  releaseCandidateSummary(): Promise<{ apiFreeze?: ReleaseApiFreezeRecord | null; compatibilityMatrix?: ReleaseCompatibilityMatrixRecord | null; upgradePlan?: ReleaseUpgradePlanRecord | null; upgradeRun?: ReleaseUpgradeRunRecord | null; readinessRun?: ReleaseCandidateReadinessRunRecord | null; openBlockers: number; openFindings: number }> {
    return this.request('/api/release-candidate/summary');
  }

  createApiFreeze(input: { version?: string; channel?: string; sourcePath?: string; rootDir?: string; status?: string; notes?: string | null } = {}): Promise<{ freeze: ReleaseApiFreezeRecord; payload: unknown }> {
    return this.request('/api/release-candidate/api-freeze/snapshots', { method: 'POST', body: input });
  }

  apiFreezeSnapshots(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<ReleaseApiFreezeRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-candidate/api-freeze/snapshots${query}`);
  }

  verifyApiFreeze(input: { freezeId?: string; version?: string; channel?: string; sourcePath?: string; rootDir?: string; allowAdditive?: boolean } = {}): Promise<{ check: unknown; report: unknown }> {
    return this.request('/api/release-candidate/api-freeze/verify', { method: 'POST', body: input });
  }

  seedCompatibilityDefaults(): Promise<{ count: number; targets: ReleaseCompatibilityTargetRecord[] }> {
    return this.request('/api/release-candidate/compatibility/defaults', { method: 'POST' });
  }

  compatibilityTargets(input: { status?: string; take?: number } = {}): Promise<ReleaseCompatibilityTargetRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-candidate/compatibility/targets${query}`);
  }

  upsertCompatibilityTarget(input: { code: string; kind: string; name: string; versionRange: string; required?: boolean; status?: string; notes?: string | null; checks?: Array<Record<string, unknown>> }): Promise<ReleaseCompatibilityTargetRecord> {
    return this.request('/api/release-candidate/compatibility/targets', { method: 'PUT', body: input });
  }

  buildCompatibilityMatrix(input: { version?: string; channel?: string; persist?: boolean } = {}): Promise<{ matrix?: ReleaseCompatibilityMatrixRecord | null; report: unknown }> {
    return this.request('/api/release-candidate/compatibility/matrix', { method: 'POST', body: input });
  }

  compatibilityMatrices(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<ReleaseCompatibilityMatrixRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-candidate/compatibility/matrix${query}`);
  }

  createUpgradePlan(input: { fromVersion?: string; toVersion?: string; channel?: string; hasBackup?: boolean; status?: string } = {}): Promise<{ plan: ReleaseUpgradePlanRecord; payload: unknown }> {
    return this.request('/api/release-candidate/upgrade-plans', { method: 'POST', body: input });
  }

  upgradePlans(input: { version?: string; status?: string; take?: number } = {}): Promise<ReleaseUpgradePlanRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-candidate/upgrade-plans${query}`);
  }

  runUpgradePlan(id: string, input: { mode?: 'dry-run' | 'apply'; rootDir?: string; notes?: string | null } = {}): Promise<{ run: ReleaseUpgradeRunRecord; plan: ReleaseUpgradePlanRecord; results: unknown[] }> {
    return this.request(`/api/release-candidate/upgrade-plans/${encodeURIComponent(id)}/run`, { method: 'POST', body: input });
  }

  upgradeRuns(input: { status?: string; take?: number } = {}): Promise<ReleaseUpgradeRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-candidate/upgrade-runs${query}`);
  }

  runRcReadiness(input: { version?: string; channel?: string; rootDir?: string; persist?: boolean } = {}): Promise<{ run?: ReleaseCandidateReadinessRunRecord | null; report: unknown }> {
    return this.request('/api/release-candidate/readiness-runs', { method: 'POST', body: input });
  }

  rcReadinessRuns(input: { version?: string; channel?: string; status?: string; take?: number } = {}): Promise<ReleaseCandidateReadinessRunRecord[]> {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(input)) if (value !== undefined) params.set(key, String(value));
    const query = params.size ? `?${params.toString()}` : '';
    return this.request(`/api/release-candidate/readiness-runs${query}`);
  }

  private async request<T>(path: string, options: { method?: string; body?: unknown; idempotencyKey?: string } = {}): Promise<T> {
    const headers: Record<string, string> = { ...this.authHeaders() };
    let body: string | undefined;
    if (options.body !== undefined) {
      headers['content-type'] = 'application/json';
      body = JSON.stringify(options.body);
    }
    if (options.idempotencyKey) headers['idempotency-key'] = options.idempotencyKey;
    const response = await fetch(new URL(path, this.options.baseUrl), { method: options.method ?? 'GET', headers, body, redirect: 'error', signal: AbortSignal.timeout(60_000) });
    if (!response.ok) throw new Error(`CajeerBots API error: ${response.status} ${await response.text()}`);
    return (await response.json()) as T;
  }

  private authHeaders(): Record<string, string> {
    if (this.options.apiKey) return { 'x-api-key': this.options.apiKey };
    if (this.options.jwt) return { authorization: `Bearer ${this.options.jwt}` };
    return {};
  }
}
