import { z } from 'zod';

export const listDeploymentTargetsQuerySchema = z.object({
  kind: z.string().optional(),
  environment: z.string().optional(),
  status: z.string().optional(),
  take: z.coerce.number().int().min(1).max(500).default(100),
});

export const upsertDeploymentTargetSchema = z.object({
  code: z.string().min(1).max(120).regex(/^[a-z0-9][a-z0-9._-]*$/i),
  title: z.string().min(1).max(200),
  kind: z.enum(['docker_compose', 'systemd', 'custom']).default('docker_compose'),
  environment: z.string().min(1).max(50).default('production'),
  baseUrl: z.string().url().optional().nullable(),
  composeFiles: z.array(z.string().min(1)).default(['docker-compose.prod.yml']),
  envFile: z.string().optional().nullable(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export const runDeploymentCheckSchema = z.object({
  rootDir: z.string().min(1).default(process.cwd()),
  composeFiles: z.array(z.string().min(1)).default(['docker-compose.prod.yml', '.env.production']),
});

export const listDeploymentChecksQuerySchema = z.object({
  targetId: z.string().optional(),
  status: z.string().optional(),
  severity: z.string().optional(),
  take: z.coerce.number().int().min(1).max(1000).default(200),
});

export const createDeploymentRunSchema = z.object({
  targetId: z.string().optional().nullable(),
  action: z.string().min(1).max(120),
  status: z.string().min(1).max(50).default('running'),
  logs: z.array(z.unknown()).optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export const finishDeploymentRunSchema = z.object({
  status: z.enum(['success', 'failed', 'cancelled']),
  error: z.string().optional().nullable(),
  logs: z.array(z.unknown()).optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export const renderEnvSchema = z.object({
  appUrl: z.string().url().optional(),
  databaseUrl: z.string().optional(),
  redisUrl: z.string().optional(),
  jwtSecret: z.string().optional(),
  adminToken: z.string().optional(),
  encryptionKey: z.string().optional(),
  botWebhookPublicUrl: z.string().url().optional(),
  storageDriver: z.enum(['local', 's3']).optional(),
  s3Endpoint: z.string().optional(),
  s3Bucket: z.string().optional(),
});

export const validateEnvSchema = z.object({
  envText: z.string().min(1),
});

export const renderNginxSchema = z.object({
  serverName: z.string().min(1),
  publicRoot: z.string().optional(),
  apiUpstream: z.string().optional(),
  botWebhookUpstream: z.string().optional(),
  enableTls: z.boolean().default(false),
  certificatePath: z.string().optional(),
  certificateKeyPath: z.string().optional(),
  clientMaxBodySize: z.string().optional(),
});

export const renderSystemdSchema = z.object({
  workdir: z.string().default('/opt/cajeerbots'),
  envFile: z.string().default('/etc/cajeerbots/cajeerbots.env'),
});

export const buildReleaseManifestSchema = z.object({
  product: z.string().default('cajeerbots'),
  version: z.string().regex(/^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/),
  channel: z.enum(['alpha', 'beta', 'rc', 'stable', 'lts', 'security']).default('alpha'),
  commit: z.string().optional(),
  createdBy: z.string().optional(),
  files: z.array(z.object({
    path: z.string().min(1),
    sizeBytes: z.number().int().min(0),
    sha256: z.string().regex(/^[a-f0-9]{64}$/i),
    contentType: z.string().optional(),
  })),
});
