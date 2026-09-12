import { Module } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { HealthController } from './health/health.controller.js';
import { SystemController } from './observability/system.controller.js';
import { AuthController } from './auth/auth.controller.js';
import { AuthService } from './auth/auth.service.js';
import { ExtensionsController } from './extensions/extensions.controller.js';
import { ExtensionsService } from './extensions/extensions.service.js';
import { WebhooksController } from './webhooks/webhooks.controller.js';
import { WebhooksService } from './webhooks/webhooks.service.js';
import { CommunitiesController } from './communities/communities.controller.js';
import { CommunitiesService } from './communities/communities.service.js';
import { UsersController } from './users/users.controller.js';
import { UsersService } from './users/users.service.js';
import { RolesController } from './roles/roles.controller.js';
import { RolesService } from './roles/roles.service.js';
import { BotAccountsController } from './bot-accounts/bot-accounts.controller.js';
import { BotAccountsService } from './bot-accounts/bot-accounts.service.js';
import { BotRuntimeController } from './bot-runtime/bot-runtime.controller.js';
import { BotRuntimeService } from './bot-runtime/bot-runtime.service.js';
import { MessageTemplatesController } from './message-templates/message-templates.controller.js';
import { MessageTemplatesService } from './message-templates/message-templates.service.js';
import { MessageCoreController } from './message-core/message-core.controller.js';
import { MessageCoreService } from './message-core/message-core.service.js';
import { SettingsController } from './settings/settings.controller.js';
import { SettingsService } from './settings/settings.service.js';
import { AuditController } from './audit/audit.controller.js';
import { WebhookEndpointsController } from './webhooks/webhook-endpoints.controller.js';
import { WebhookEndpointsService } from './webhooks/webhook-endpoints.service.js';
import { OutboxController } from './outbox/outbox.controller.js';
import { OutboxService } from './outbox/outbox.service.js';
import { SecretsController } from './secrets/secrets.controller.js';
import { SecretsService } from './secrets/secrets.service.js';
import { RuntimeConfigController } from './config/config.controller.js';
import { RuntimeConfigService } from './config/config.service.js';
import { ImportExportController } from './import-export/import-export.controller.js';
import { ImportExportService } from './import-export/import-export.service.js';
import { InteractionsController } from './interactions/interactions.controller.js';
import { InteractionsService } from './interactions/interactions.service.js';
import { ScopedAuthGuard } from './shared/scoped-auth.guard.js';
import { SecurityController } from './security/security.controller.js';
import { SecurityService } from './security/security.service.js';
import { EventApiController } from './event-api/event-api.controller.js';
import { EventApiService } from './event-api/event-api.service.js';
import { RegistryController } from './registry/registry.controller.js';
import { RegistryService } from './registry/registry.service.js';
import { UpdatesController } from './updates/updates.controller.js';
import { UpdatesService } from './updates/updates.service.js';
import { JobsController } from './jobs/jobs.controller.js';
import { JobsService } from './jobs/jobs.service.js';
import { DevtoolsController } from './devtools/devtools.controller.js';
import { DevtoolsService } from './devtools/devtools.service.js';
import { BackupsController, CommunityBackupsController } from './backups/backups.controller.js';
import { StorageController } from './backups/storage.controller.js';
import { BackupsService } from './backups/backups.service.js';
import { ObservabilityController } from './observability/observability.controller.js';
import { ObservabilityService } from './observability/observability.service.js';
import { DeploymentController } from './deployment/deployment.controller.js';
import { DeploymentService } from './deployment/deployment.service.js';
import { ContractsController } from './contracts/contracts.controller.js';
import { ContractsService } from './contracts/contracts.service.js';
import { DocsController } from './docs/docs.controller.js';
import { DocsService } from './docs/docs.service.js';
import { ReleaseGatesController } from './release-gates/release-gates.controller.js';
import { ReleaseGatesService } from './release-gates/release-gates.service.js';
import { IntegrationTestsController } from './integration-tests/integration-tests.controller.js';
import { IntegrationTestsService } from './integration-tests/integration-tests.service.js';
import { ReleaseCandidateController } from './release-candidate/release-candidate.controller.js';
import { ReleaseCandidateService } from './release-candidate/release-candidate.service.js';

@Module({
  controllers: [
    HealthController,
    SystemController,
    AuthController,
    CommunitiesController,
    UsersController,
    RolesController,
    BotAccountsController,
    BotRuntimeController,
    SettingsController,
    MessageTemplatesController,
    MessageCoreController,
    ExtensionsController,
    WebhooksController,
    WebhookEndpointsController,
    OutboxController,
    SecretsController,
    RuntimeConfigController,
    ImportExportController,
    InteractionsController,
    AuditController,
    SecurityController,
    EventApiController,
    RegistryController,
    UpdatesController,
    JobsController,
    DevtoolsController,
    BackupsController,
    CommunityBackupsController,
    StorageController,
    ObservabilityController,
    DeploymentController,
    ContractsController,
    DocsController,
    ReleaseGatesController,
    IntegrationTestsController,
    ReleaseCandidateController,
  ],
  providers: [
    PrismaService,
    ScopedAuthGuard,
    AuthService,
    CommunitiesService,
    UsersService,
    RolesService,
    BotAccountsService,
    BotRuntimeService,
    SettingsService,
    MessageTemplatesService,
    MessageCoreService,
    ExtensionsService,
    WebhooksService,
    WebhookEndpointsService,
    OutboxService,
    SecretsService,
    RuntimeConfigService,
    ImportExportService,
    InteractionsService,
    SecurityService,
    EventApiService,
    RegistryService,
    UpdatesService,
    JobsService,
    DevtoolsService,
    BackupsService,
    ObservabilityService,
    DeploymentService,
    ContractsService,
    DocsService,
    ReleaseGatesService,
    IntegrationTestsService,
    ReleaseCandidateService,
  ],
})
export class AppModule {}
